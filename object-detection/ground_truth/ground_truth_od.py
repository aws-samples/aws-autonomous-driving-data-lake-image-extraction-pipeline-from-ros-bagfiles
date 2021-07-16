"""Define classes and functions for interfacing with SageMaker Ground
Truth object detection.

"""

import os
import imageio
import matplotlib.pyplot as plt
import numpy as np


class BoundingBox:
    """Bounding box for an object in an image."""

    def __init__(self, image_id=None, boxdata=None):
        self.image_id = image_id
        if boxdata:
            for datum in boxdata:
                setattr(self, datum, boxdata[datum])

    def __repr__(self):
        return "Box for image {}".format(self.image_id)

    def compute_bb_data(self):
        """Compute the parameters used for IoU."""
        image = self.image
        self.xmin = self.left / image.width
        self.xmax = (self.left + self.width) / image.width
        self.ymin = self.top / image.height
        self.ymax = (self.top + self.height) / image.height


class BoxedImage:
    """Image with bounding boxes."""

    def __init__(
        self,
        id=None,
        consolidated_boxes=None,
        worker_boxes=None,
        gt_boxes=None,
        uri=None,
        size=None,
    ):
        self.id = id
        self.uri = uri
        if uri:
            self.filename = uri.split("/")[-1]
            self.oid_id = self.filename.split(".")[0]
        else:
            self.filename = None
            self.oid_id = None
        self.local = None
        self.im = None
        if size:
            self.width = size["width"]
            self.depth = size["depth"]
            self.height = size["height"]
            self.shape = self.width, self.height, self.depth
        if consolidated_boxes:
            self.consolidated_boxes = consolidated_boxes
        else:
            self.consolidated_boxes = []
        if worker_boxes:
            self.worker_boxes = worker_boxes
        else:
            self.worker_boxes = []
        if gt_boxes:
            self.gt_boxes = gt_boxes
        else:
            self.gt_boxes = []

    def __repr__(self):
        return "Image{}".format(self.id)

    def n_consolidated_boxes(self):
        """Count the number of consolidated boxes."""
        return len(self.consolidated_boxes)

    def n_worker_boxes(self):
        return len(self.worker_boxes)

    def download(self, directory):
        target_fname = os.path.join(directory, self.uri.split("/")[-1])
        if not os.path.isfile(target_fname):
            os.system(f"aws s3 cp {self.uri} {target_fname}")
            print("downloading image to {}".format(target_fname))
        self.local = target_fname

    def imread(self):
        """Cache the image reading process."""
        try:
            return imageio.imread(self.local)#, exifrotate=False)
        except OSError:
            print(
                "You need to download this image {} first. "
                "Use this_image.download(local_directory).".format(self.local)
            )
            raise

    def plot_bbs(self, ax, bbs, img_kwargs, box_kwargs, **kwargs):
        """Master function for plotting images with bounding boxes."""
        img = self.imread()
        ax.imshow(img, **img_kwargs)
        imh, imw, *_ = img.shape
        box_kwargs["fill"] = None
        class_colors = {
            0: "C0",
            1: "C1",
            2: "C2",
            3: "C3",
            4: "C4",
            5: "C5",
            6: "C6",
            7: "C7",
            8: "C8",
            9: "C9",
            10: "C10",
            11: "C11",
            12: "C12",
            13: "C13",
            14: "C14",
            15: "C15",
            16: "C16",
        }
        for bb in bbs:
            class_id = bb.class_id
            if class_id not in class_colors:
                class_colors[class_id] = "C" + str(class_id)
            rec = plt.Rectangle(
                (bb.left, bb.top),
                bb.width,
                bb.height,
                linewidth=1,
                edgecolor=class_colors[class_id],
                **box_kwargs,
            )
            if bb.class_id == 0:
                annotation = "car-" + str(bb.confidence)
            else:
                annotation = str(bb.class_id) + "-" + str(bb.confidence)
            ax.text(
                bb.left,
                (bb.top + bb.height),
                annotation,
                fontsize=6,
                verticalalignment="center",
                bbox=dict(facecolor=class_colors[class_id], alpha=0.5),
            )
            ax.add_patch(rec)
        ax.axis("off")

    def plot_consolidated_bbs(self, ax, img_kwargs={}, box_kwargs={"lw": 1}):
        """Plot the consolidated boxes."""
        self.plot_bbs(
            ax, self.consolidated_boxes, img_kwargs=img_kwargs, box_kwargs=box_kwargs
        )

    def crop_bbs(self, ax, bbs, uri, img_kwargs, box_kwargs, **kwargs):
        """Master function for cropping images around bounding boxes."""
        img = self.imread()
        uri = uri.split("/")[4]
        bb_count = 0
        for bb in bbs:
            y = int(bb.top)
            x = int(bb.left)
            h = int(bb.height)
            w = int(bb.width)
            cropped = img[y : y + h, x : x + w]
            imageio.imwrite(
                "./cropped-images/cropped-{}_{}.png".format(uri, bb_count),
                cropped,
            )
            bb_count += 1

    def plot_crop_consolidated_bbs(self, ax, img_kwargs={}, box_kwargs={"lw": 1}):
        """Plot the consolidated boxes."""
        self.plot_bbs(
            ax, self.consolidated_boxes, img_kwargs=img_kwargs, box_kwargs=box_kwargs
        )
        self.crop_bbs(
            ax,
            self.consolidated_boxes,
            self.uri,
            img_kwargs=img_kwargs,
            box_kwargs=box_kwargs,
        )

    def compute_img_confidence(self):
        """ Compute the mean bb confidence. """
        if len(self.consolidated_boxes) > 0:
            return np.mean([box.confidence for box in self.consolidated_boxes])
        else:
            return 0
