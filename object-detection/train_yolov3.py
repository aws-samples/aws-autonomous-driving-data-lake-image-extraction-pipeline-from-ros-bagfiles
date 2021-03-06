"""Train YOLOv3 with random shapes."""
import argparse
import os
import logging
import time
import warnings
import numpy as np
import mxnet as mx
print("mxnet_version: ",mx.__version__)
from mxnet import nd, gluon, autograd
import gluoncv as gcv
gcv.utils.check_version('0.8.0')
from gluoncv import data as gdata
from gluoncv import utils as gutils
from gluoncv.model_zoo import get_model
from gluoncv.data.batchify import Tuple, Stack, Pad
from gluoncv.data.transforms.presets.yolo import YOLO3DefaultTrainTransform, YOLO3DefaultValTransform
from gluoncv.data.dataloader import RandomTransformDataLoader
from gluoncv.utils.metrics.voc_detection import VOC07MApMetric
from gluoncv.utils import LRScheduler, LRSequential
import json
from matplotlib import pyplot as plt
logging.basicConfig(level=logging.DEBUG)

def parse_args():
    parser = argparse.ArgumentParser(description='Train YOLO networks with random input shape.')
    parser.add_argument('--model-dir', type=str, default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train', type=str, default=os.environ['SM_CHANNEL_TRAIN'])
    parser.add_argument('--test', type=str, default=os.environ['SM_CHANNEL_TEST'])
    parser.add_argument('--val', type=str, default=os.environ['SM_CHANNEL_VAL'])
    parser.add_argument("--checkpoint-dir",type=str,default="/opt/ml/checkpoints",help="Path where checkpoints will be saved.")
    parser.add_argument('--dataset', type=str, choices=['custom','coco','voc'],default='custom',
                        help='Training dataset. Now support voc.')
    parser.add_argument('--current-host', type=str, default=os.environ['SM_CURRENT_HOST'])
    parser.add_argument('--hosts', type=list, default=json.loads(os.environ['SM_HOSTS']))
    parser.add_argument('--network', type=str, choices=['darknet53','mobilenet1.0'], default='darknet53',
                        help="Base network name which serves as feature extraction base.")
    parser.add_argument('--data-shape', type=int, default=512,
                        help="Input data shape for evaluation, use 320, 416, 608... " +
                             "Training is with random shapes from (320 to 608).")
    parser.add_argument('--batch-size', type=int, default=24, help='Training mini-batch size')
    parser.add_argument('--num-workers', '-j', dest='num_workers', type=int,
                        default=8, help='Number of data workers, you can use larger '
                        'number to accelerate data loading, if you CPU and GPUs are powerful.')
    parser.add_argument('--gpus',  type=int, default=os.environ['SM_NUM_GPUS'],
                        help='Training with GPUs, you can specify 1,3 for example.')
    parser.add_argument('--epochs', type=int, default=1,
                        help='Training epochs.')
    parser.add_argument('--resume', type=str, default='',
                        help='Resume from previously saved parameters if not None. '
                        'For example, you can resume from ./yolo3_xxx_0123.params')
    parser.add_argument('--start-epoch', type=int, default=0,
                        help='Starting epoch for resuming, default is 0 for new training.'
                        'You can specify it to 100 for example to start from 100 epoch.')
    parser.add_argument('--lr', type=float, default=0.001,
                        help='Learning rate, default is 0.001')
    parser.add_argument('--lr-mode', type=str, default='step',
                        help='learning rate scheduler mode. options are step, poly and cosine.')
    parser.add_argument('--lr-decay', type=float, default=0.1,
                        help='decay rate of learning rate. default is 0.1.')
    parser.add_argument('--lr-decay-period', type=int, default=0,
                        help='interval for periodic learning rate decays. default is 0 to disable.')
    parser.add_argument('--lr-decay-epoch', type=str, default='160,180',
                        help='epochs at which learning rate decays. default is 160,180.')
    parser.add_argument('--warmup-lr', type=float, default=0.0,
                        help='starting warmup learning rate. default is 0.0.')
    parser.add_argument('--warmup-epochs', type=int, default=2,
                        help='number of warmup epochs.')
    parser.add_argument('--momentum', type=float, default=0.9,
                        help='SGD momentum, default is 0.9')
    parser.add_argument('--wd', type=float, default=0.0005,
                        help='Weight decay, default is 5e-4')
    parser.add_argument('--log-interval', type=int, default=100,
                        help='Logging mini-batch interval. Default is 100.')
    parser.add_argument('--save-prefix', type=str, default='',
                        help='Saving parameter prefix')
    parser.add_argument('--save-interval', type=int, default=10,
                        help='Saving parameters epoch interval, best model will always be saved.')
    parser.add_argument('--val-interval', type=int, default=5,
                        help='Epoch interval for validation, increase the number will reduce the '
                             'training time if validation is slow.')
    parser.add_argument('--seed', type=int, default=233,
                        help='Random seed to be fixed.')
    parser.add_argument('--num-samples', type=int, default=-1,
                        help='Training images. Use -1 to automatically get the number.')
    parser.add_argument('--syncbn', action='store_true',
                        help='Use synchronize BN across devices.')
    parser.add_argument('--no-random-shape', action='store_true',
                        help='Use fixed size(data-shape) throughout the training, which will be faster '
                        'and require less memory. However, final model will be slightly worse.')
    parser.add_argument('--no-wd', action='store_true',
                        help='whether to remove weight decay on bias, and beta/gamma for batchnorm layers.')
    parser.add_argument('--mixup', type=bool, default=True,
                        help='whether to enable mixup.')
    parser.add_argument('--no-mixup-epochs', type=int, default=20,
                        help='Disable mixup training if enabled in the last N epochs.')
    parser.add_argument('--pretrained-model', type=str, choices=['Coco', 'None'], default='Coco',
                       help='Use a pre-trained model on Coco')
    parser.add_argument('--label-smooth', action='store_true', help='Use label smoothing.')
    args = parser.parse_args()
    return args


def get_dataset(args): 
    """loads the .rec and .idx files from the specified in the arguments and initi"""
    train_dataset = gcv.data.RecordFileDetection(args.train+ '/train.rec', coord_normalized=True)
    val_dataset = gcv.data.RecordFileDetection(args.val+ '/val.rec', coord_normalized=True)
    test_dataset = gcv.data.RecordFileDetection(args.test+ '/test.rec', coord_normalized=True)
    classes = ['car']
    val_metric = VOC07MApMetric(iou_thresh=0.5, class_names=classes)
    
    if args.num_samples < 0:
        args.num_samples = len(train_dataset)
    if args.mixup:
        from gluoncv.data import MixupDetection
        train_dataset = MixupDetection(train_dataset)
    return train_dataset, val_dataset,test_dataset, val_metric

def get_dataloader(net, train_dataset, val_dataset, test_dataset, data_shape, batch_size, num_workers, args):
    """Get dataloader."""
    if train_dataset is not None:
            width, height = data_shape, data_shape

            batchify_fn = Tuple(*([Stack() for _ in range(6)] + [Pad(axis=0, pad_val=-1) for _ in range(1)]))  # stack image, all targets generated
            train_loader = mx.gluon.data.DataLoader(
                                train_dataset.transform(YOLO3DefaultTrainTransform(width, height, net,mixup=False)),
                                batch_size, True, batchify_fn=batchify_fn, last_batch='rollover', num_workers=num_workers)

            val_batchify_fn = Tuple(Stack(), Pad(pad_val=-1))
            val_loader = mx.gluon.data.DataLoader(val_dataset.transform(YOLO3DefaultValTransform(width, height)),
                            batch_size, False, batchify_fn=val_batchify_fn, last_batch='keep', num_workers=num_workers)
            test_batchify_fn = Tuple(Stack(), Pad(pad_val=-1))
            test_loader = mx.gluon.data.DataLoader(test_dataset.transform(YOLO3DefaultValTransform(width, height)),
                            batch_size, False, batchify_fn=test_batchify_fn, last_batch='keep', num_workers=num_workers)
            return train_loader, val_loader, test_loader

def save_params(net, best_map, current_map, epoch, save_interval, prefix, checkpoint_dir):
    """saving model parameters in case the mAP has improved"""
    current_map = float(current_map)
    if current_map > best_map[0]:
        logging.info('current_map {} > best_map {}]'.format(current_map,best_map[0]))
        best_map[0] = current_map
        print('{:s}_best.params'.format(checkpoint_dir,prefix, epoch, current_map))
        net.save_parameters('{:s}_best.params'.format(checkpoint_dir,prefix, epoch, current_map))
        with open(prefix+'_best_map.log', 'a') as f:
            f.write('{:04d}:\t{:.4f}\n'.format(epoch, current_map))
    if save_interval and epoch % save_interval == 0:
        net.save_parameters('{:s}_{:s}_{:04d}_{:.4f}.params'.format(checkpoint_dir,prefix, epoch, current_map))

def validate(net, val_data, ctx, eval_metric):
    """Test on validation dataset."""
    eval_metric.reset()
    # set nms threshold and topk constraint
    net.set_nms(nms_thresh=0.45, nms_topk=400)
    mx.nd.waitall()
    net.hybridize()

    for batch in val_data:
        data = gluon.utils.split_and_load(batch[0], ctx_list=ctx, batch_axis=0, even_split=False)
        label = gluon.utils.split_and_load(batch[1], ctx_list=ctx, batch_axis=0, even_split=False)
        det_bboxes = []
        det_ids = []
        det_scores = []
        gt_bboxes = []
        gt_ids = []        
        gt_difficults = []
        for x, y in zip(data, label):
            # get prediction resultsx
            ids, scores, bboxes = net(x)
            det_ids.append(ids)
            det_scores.append(scores)
            # clip to image size
            det_bboxes.append(bboxes.clip(0, batch[0].shape[2]))
            # split ground truths
            gt_ids.append(y.slice_axis(axis=-1, begin=4, end=5))
            gt_bboxes.append(y.slice_axis(axis=-1, begin=0, end=4))
            gt_difficults.append(y.slice_axis(axis=-1, begin=5, end=6) if y.shape[-1] > 5 else None)
        # update metric        
        eval_metric.update(det_bboxes, det_ids, det_scores, gt_bboxes, gt_ids,gt_difficults)
    return eval_metric.get()

def train(net, train_data, val_data, eval_metric, ctx, args):
    """Training pipeline"""
    if args.no_wd:
        for k, v in net.collect_params('.*beta|.*gamma|.*bias').items():
            v.wd_mult = 0.0

    if args.label_smooth:
        net._target_generator._label_smooth = True

    if args.lr_decay_period > 0:
        lr_decay_epoch = list(range(args.lr_decay_period, args.epochs, args.lr_decay_period))
    else:
        lr_decay_epoch = [int(i) for i in args.lr_decay_epoch.split(',')]
    
    lr_scheduler = LRSequential([
        LRScheduler('linear', base_lr=0, target_lr=args.lr,
                    nepochs=args.warmup_epochs, iters_per_epoch=args.batch_size),
        LRScheduler(args.lr_mode, base_lr=args.lr,
                    nepochs=args.epochs - args.warmup_epochs,
                    iters_per_epoch=args.batch_size,
                    step_epoch=lr_decay_epoch,
                    step_factor=args.lr_decay, power=2),
    ])

    trainer = gluon.Trainer(
        net.collect_params(), 'sgd',
        {'wd': args.wd, 'momentum': args.momentum, 'lr_scheduler': lr_scheduler},
        kvstore='local')

    # targets
    sigmoid_ce = gluon.loss.SigmoidBinaryCrossEntropyLoss(from_sigmoid=False)
    l1_loss = gluon.loss.L1Loss()

    # metrics
    obj_metrics = mx.metric.Loss('ObjLoss')
    center_metrics = mx.metric.Loss('BoxCenterLoss')
    scale_metrics = mx.metric.Loss('BoxScaleLoss')
    cls_metrics = mx.metric.Loss('ClassLoss')

    logging.info('Start training from [Epoch {}]'.format(args.start_epoch))
    best_map = [0]
    for epoch in range(args.start_epoch, args.epochs):
        if args.mixup:
            # TODO(zhreshold): more elegant way to control mixup during runtime
            try:
                train_data._dataset.set_mixup(np.random.beta, 1.5, 1.5)
            except AttributeError:
                train_data._dataset._data.set_mixup(np.random.beta, 1.5, 1.5)
            if epoch >= args.epochs - args.no_mixup_epochs:
                try:
                    train_data._dataset.set_mixup(None)
                except AttributeError:
                    train_data._dataset._data.set_mixup(None)

        tic = time.time()
        btic = time.time()
        mx.nd.waitall()
        net.hybridize()
        for i, batch in enumerate(train_data):
            batch_size = batch[0].shape[0]
            data = gluon.utils.split_and_load(batch[0], ctx_list=ctx, batch_axis=0)
            # objectness, center_targets, scale_targets, weights, class_targets
            fixed_targets = [gluon.utils.split_and_load(batch[it], ctx_list=ctx, batch_axis=0) for it in range(1, 6)]
            gt_boxes = gluon.utils.split_and_load(batch[6], ctx_list=ctx, batch_axis=0)
            sum_losses = []
            obj_losses = []
            center_losses = []
            scale_losses = []
            cls_losses = []
            with autograd.record():
                for ix, x in enumerate(data):
                    obj_loss, center_loss, scale_loss, cls_loss = net(x, gt_boxes[ix], *[ft[ix] for ft in fixed_targets])
                    sum_losses.append(obj_loss + center_loss + scale_loss + cls_loss)
                    obj_losses.append(obj_loss)
                    center_losses.append(center_loss)
                    scale_losses.append(scale_loss)
                    cls_losses.append(cls_loss)
                autograd.backward(sum_losses)            
            trainer.step(args.batch_size)
            obj_metrics.update(0, obj_losses)
            center_metrics.update(0, center_losses)
            scale_metrics.update(0, scale_losses)
            cls_metrics.update(0, cls_losses)
            #if args.log_interval and not (i + 1) % args.log_interval:
            #    name1, loss1 = obj_metrics.get()
            #    name2, loss2 = center_metrics.get()
            #    name3, loss3 = scale_metrics.get()
            #    name4, loss4 = cls_metrics.get()
            #    logging.info('[Epoch {}][Batch {}], LR: {:.2E}, Speed: {:.3f} samples/sec, {}={:.3f}, {}={:.3f}, {}={:.3f}, {}={:.3f},'.format(
            #        epoch, i, trainer.learning_rate, batch_size/(time.time()-btic), name1, loss1, name2, loss2, name3, loss3, name4, loss4))
            btic = time.time()

        name1, loss1 = obj_metrics.get()
        name2, loss2 = center_metrics.get()
        name3, loss3 = scale_metrics.get()
        name4, loss4 = cls_metrics.get()
        logging.info('[Epoch {}] Training time: {:.3f}, {}={:.3f}, {}={:.3f}, {}={:.3f}, {}={:.3f},'.format(
            epoch, (time.time()-tic), name1, loss1, name2, loss2, name3, loss3, name4, loss4))
        if not (epoch + 1) % args.val_interval:
            # consider reduce the frequency of validation to save time
            map_name, mean_ap = validate(net, val_data, ctx, eval_metric)
            val_msg = '\n'.join(['{}={},'.format(k, v) for k, v in zip(["val:" + metric for metric in map_name], mean_ap)])
            logging.info('[Epoch {}] Validation: \n{}'.format(epoch, val_msg))
            current_map = float(mean_ap[-1])            
        else:
            current_map = 0.
        save_params(net, best_map, current_map, epoch, args.save_interval, args.save_prefix, args.checkpoint_dir)            
    print("saved to: ")
    print('{:s}/model'.format(args.model_dir))
    net.export(path='{:s}/model'.format(args.model_dir))

# ------------------------------------------------------------ #
# Hosting methods                                              #
# ------------------------------------------------------------ #

def get_ctx():
    "function to get machine hardware context"
    try:
        _ = mx.nd.array([0], ctx=mx.gpu())
        ctx = mx.gpu()
    except:
        try:
            _ = mx.nd.array([0], ctx=mx.eia())
            ctx = mx.eia()
        except: 
            ctx = mx.cpu()
    return ctx


def model_fn(model_dir):
    """
    Load the gluon model. Called once when hosting service starts.
    :param: model_dir The directory where model files are stored.
    :return: a model (in this case a Gluon network)
    """
    logging.info('Invoking user-defined model_fn')

    import neomx 
    logging.info('MXNet version used for model loading {}'.format(mx.__version__))

    #select CPU/GPU context
    ctx = get_ctx()

    net = gluon.SymbolBlock.imports(
        '%s/compiled-symbol.json' % model_dir,
        ['data'],
        '%s/compiled-0000.params' % model_dir,
        ctx=ctx
    )
    net.hybridize(static_alloc=True, static_shape=True)

    #run warm-up inference on empty data
    warmup_data = mx.nd.empty((1,3,512,512), ctx=ctx)
    class_IDs, scores, bounding_boxes = net(warmup_data)
   
    return net

def transform_fn(net, data, input_content_type, output_content_type):
    """
    Transform a request using the Gluon model. Called once per request.
    :param net: The Gluon model.
    :param data: The request payload.
    :param input_content_type: The request content type.
    :param output_content_type: The (desired) response content type.
    :return: response payload and content type.
    """
    logging.info("Invoking user defined transform_fn")
    
    import gluoncv as gcv
    #change context to mx.cpu() when optimizing and deploying with Neo for CPU endpoints
    ctx = get_ctx()
    # we can use content types to vary input/output handling, but
    # here we just assume json for both
    data = json.loads(data)
    #preprocess image  
    x, image = gcv.data.transforms.presets.yolo.transform_test(mx.nd.array(data), 512)
    #load image onto right context
    x = x.as_in_context(ctx)
    class_IDs, scores, bounding_boxes = net(x)
    #create list of results
    result = [class_IDs.asnumpy().tolist(), scores.asnumpy().tolist(), bounding_boxes.asnumpy().tolist()]
    
    #decode as json string
    response_body = json.dumps(result)
    
    return response_body, output_content_type
        
# ------------------------------------------------------------ #
# Training execution                                           #
# ------------------------------------------------------------ #


if __name__ == '__main__':
    args = parse_args()
    # fix seed for mxnet, numpy and python builtin random generator.
    gutils.random.seed(args.seed)

    # training contexts
    if args.gpus > 0:
        ctx = [mx.gpu(int(i)) for i in list(range(0,args.gpus))]
    else: 
        ctx = ctx if ctx else [mx.cpu()]

    print("ctx: ",ctx)

    # network
    net_name = '_'.join(('yolo3', args.network, args.dataset)) 
    logging.info('net_name: {}'.format(net_name)) 
    args.save_prefix += net_name
    # use sync bn if specified
    num_sync_bn_devices = len(ctx) if args.syncbn else -1
    classes = ['car']
    if args.syncbn and len(ctx) > 1:
        net = get_model(net_name, pretrained_base=False, transfer='coco', norm_layer=gluon.contrib.nn.SyncBatchNorm,
                        norm_kwargs={'num_devices': len(num_sync_bn_devices)},classes=classes)
        async_net = get_model(net_name, pretrained_base=False, transfer='coco',classes=classes)  # used by cpu worker
    else:
        if args.pretrained_model == 'Coco':
            logging.info('using Coco pre-trained model')
            net = get_model(net_name,norm_layer=gluon.nn.BatchNorm,
    classes=classes,pretrained_base=False, transfer='coco')
        else:
            logging.info('training model from scratch - no pre-trained model is used.')
            net = get_model(net_name,norm_layer=gluon.nn.BatchNorm,pretrained_base=False)
        async_net = net

    if args.resume.strip():
        net.load_parameters(args.resume.strip())
        async_net.load_parameters(args.resume.strip())
    else:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            net.initialize()
            async_net.initialize()

    # training data
    train_dataset, val_dataset, test_dataset, eval_metric = get_dataset(args)
    train_data, val_data, test_data = get_dataloader(
        async_net, train_dataset, val_dataset, test_dataset, args.data_shape, args.batch_size, args.num_workers, args)
    net.collect_params().reset_ctx(ctx)
    # No Transfer Learning
    map_name, mean_ap = validate(net, test_data, ctx, eval_metric)
    val_msg = '\n'.join(['{}={},'.format(k, v) for k, v in zip(["test:" + metric for metric in map_name], mean_ap)])
    logging.info('Performance on test set before finetuning: \n{}'.format(val_msg))
    
    start_time_train = time.time()
    # training
    train(net, train_data, val_data, eval_metric, ctx, args)
    logging.info("--- %s training seconds ---" % (time.time() - start_time_train))
    # After Transfer Learning
    start_time_test= time.time()
    map_name, mean_ap = validate(net, test_data, ctx, eval_metric)
    speed = len(test_data) / (time.time() - start_time_test)
    print('Throughput is %f img/sec.'% speed)
    val_msg = '\n'.join(['{}={},'.format(k, v) for k, v in zip(["test:" + metric for metric in map_name], mean_ap)])
    logging.info('Performance on test set after finetuning: \n{}'.format(val_msg))