from importRosbag.importRosbag import importRosbag
import pandas as pd


if __name__ == "__main__":
    acceptable_topics = [
        "/gps",
        "/gps_time",
        "/imu",
        "/pose_ground_truth",
        "/pose_localized",
        "/pose_raw",
        "/tf",
        "/velocity_raw",
    ]

    # topics = importRosbag(filePathOrName='/app/Sample-Data.bag', listTopics=True)
    # print(topics)
    data = importRosbag(
        filePathOrName="/app/Sample-Data.bag",
        importTypes=["sensor_msgs_Imu"],
    )

    dfs = []
    for k, v in data["/imu"].items():
        print(k)
        if isinstance(v, str):
            print(v)
        else:
            dfs.append(pd.DataFrame.from_dict(v))

    print(pd.concat(dfs, axis=1).head())
    print(pd.concat(dfs, axis=1).shape)
