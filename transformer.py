from datetime import datetime
from json import JSONDecoder
from os import makedirs
from os import path as pth
from os import walk
from random import sample
from secrets import token_hex
from socket import gethostname
from datetime import datetime

D_MAP = {"i": "1", "n": "2", "f": "3"}
NUMBER_DEVICES = 100000
LENGHT_DIRECTORY_DEVICES = 10


def run(log_location, base_dir):
    cache = set([])
    file_cache = {}
    expected_keys = {"i", "o", "d", "p", "t", "r", "w"}
    # log_location = 'biglog.log'
    # log_location = '/var/log/nginx/coronaviruscheck.org/postdata.log.1'

    with open(log_location) as json_file:
        decoder = JSONDecoder()
        batch_number = "/" + datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + '_' + token_hex(15) + ".csv"
        for line in json_file:
            if line != "\n":
                try:
                    log_data = decoder.raw_decode(line)
                except:
                    continue
                bulk_write = {}

                if 'z' in log_data[0]:
                    for interaction in log_data[0]['z']:
                        event_date = datetime.fromtimestamp(int(interaction["w"]))
                        base_path = [
                            base_dir,
                            event_date.year,
                            str(event_date.month).zfill(2),
                            str(event_date.day).zfill(2),
                        ]

                        my_id = str(int(log_data[0]['i'] / NUMBER_DEVICES)).zfill(
                            LENGHT_DIRECTORY_DEVICES
                        )
                        other_id = str(int(interaction["o"] / NUMBER_DEVICES)).zfill(
                            LENGHT_DIRECTORY_DEVICES
                        )

                        data_to_save1 = [
                            log_data[0]['i'],
                            interaction["o"],
                            interaction["w"],
                            int(interaction["w"] + interaction["t"]),
                            interaction["t"],
                            interaction["r"],
                            str(interaction["s"]).replace(",", ".")
                        ]
                        data_to_save2 = [
                            interaction["o"],
                            log_data[0]['i'],
                            interaction["w"],
                            int(interaction["w"] + interaction["t"]),
                            interaction["t"],
                            interaction["r"],
                            str(interaction["s"]).replace(",", ".")
                        ]

                        if 'x' in interaction:
                            data_to_save1.append(str(interaction['x']).replace(",", "."))
                            data_to_save2.append(str(interaction['x']).replace(",", "."))
                        else:
                            data_to_save1.append('')
                            data_to_save2.append('')

                        if 'y' in interaction:
                            data_to_save1.append(str(interaction['y']).replace(",", "."))
                            data_to_save2.append(str(interaction['y']).replace(",", "."))
                        else:
                            data_to_save1.append('')
                            data_to_save2.append('')

                        data_to_save1.append(log_data[0]['p'])
                        data_to_save2.append(log_data[0]['p'])

                        hostname = gethostname()
                        path_to_file1 = base_path.copy()
                        path_to_file2 = base_path.copy()

                        for index in range(0, 9, 2):
                            path_to_file1.append(str(my_id[index] + my_id[index + 1]))
                            path_to_file2.append(
                                str(other_id[index] + other_id[index + 1])
                            )

                        path_to_file1.append(hostname)
                        path_to_file2.append(hostname)

                        path_to_file1 = "/".join(map(str, path_to_file1))
                        path_to_file2 = "/".join(map(str, path_to_file2))

                        if path_to_file1 not in cache:
                            makedirs(path_to_file1)
                            cache.add(path_to_file1)

                        if path_to_file2 not in cache:
                            makedirs(path_to_file2)
                            cache.add(path_to_file2)

                        path_to_file1 += batch_number
                        path_to_file2 += batch_number

                        csv1 = ",".join(map(str, data_to_save1)) + "\n"
                        csv2 = ",".join(map(str, data_to_save2)) + "\n"

                        if path_to_file1 in bulk_write:
                            bulk_write[path_to_file1] = bulk_write[path_to_file1] + csv1
                        else:
                            bulk_write[path_to_file1] = csv1
                        if path_to_file2 in bulk_write:
                            bulk_write[path_to_file2] = bulk_write[path_to_file2] + csv2
                        else:
                            bulk_write[path_to_file2] = csv2
                else:
                    for event in log_data[0]:
                        if all(key in event for key in expected_keys):
                            event_date = datetime.fromtimestamp(event.get("w"))
                            base_path = [
                                base_dir,
                                event_date.year,
                                str(event_date.month).zfill(2),
                                str(event_date.day).zfill(2),
                            ]

                            id_my = event.get("i")
                            id_other = event.get("o")

                            my_id = str(int(id_my / NUMBER_DEVICES)).zfill(
                                LENGHT_DIRECTORY_DEVICES
                            )
                            other_id = str(int(id_other / NUMBER_DEVICES)).zfill(
                                LENGHT_DIRECTORY_DEVICES
                            )
                            timestamp_start = event["w"]
                            timestamp_end = event["w"] + event["t"]
                            duration = event["t"]
                            distance_type = D_MAP[str(event["d"])]
                            rssi = event["r"]
                            latitude = event["x"]
                            longitude = event["y"]
                            platform = event["p"]

                            hostname = gethostname()
                            path_to_file1 = base_path.copy()
                            path_to_file2 = base_path.copy()

                            for index in range(0, 9, 2):
                                path_to_file1.append(str(my_id[index] + my_id[index + 1]))
                                path_to_file2.append(
                                    str(other_id[index] + other_id[index + 1])
                                )

                            path_to_file1.append(hostname)
                            path_to_file2.append(hostname)

                            path_to_file1 = "/".join(map(str, path_to_file1))
                            path_to_file2 = "/".join(map(str, path_to_file2))

                            if path_to_file1 not in cache:
                                makedirs(path_to_file1)
                                cache.add(path_to_file1)

                            if path_to_file2 not in cache:
                                makedirs(path_to_file2)
                                cache.add(path_to_file2)

                            path_to_file1 += batch_number
                            path_to_file2 += batch_number

                            # my_id, other_id, timestamp_start, timestamp_end, interaction_lenght_in_seconds,
                            # RSSI, distance_type, latitude, longitude,
                            data_to_save1 = [
                                id_my,
                                id_other,
                                timestamp_start,
                                timestamp_end,
                                duration,
                                rssi,
                                distance_type,
                                latitude,
                                longitude,
                                platform
                            ]
                            data_to_save2 = [
                                id_other,
                                id_my,
                                timestamp_start,
                                timestamp_end,
                                duration,
                                rssi,
                                distance_type,
                                latitude,
                                longitude,
                                platform
                            ]

                            csv1 = ",".join(map(str, data_to_save1)) + "\n"
                            csv2 = ",".join(map(str, data_to_save2)) + "\n"

                            if path_to_file1 in bulk_write:
                                bulk_write[path_to_file1] = bulk_write[path_to_file1] + csv1
                            else:
                                bulk_write[path_to_file1] = csv1
                            if path_to_file2 in bulk_write:
                                bulk_write[path_to_file2] = bulk_write[path_to_file2] + csv2
                            else:
                                bulk_write[path_to_file2] = csv2

                for path, csv in bulk_write.items():
                    if path in file_cache:
                        f = file_cache[path]
                    else:
                        f = open(path, "a+")
                        file_cache[path] = f
                    f.write(csv)
                    if len(file_cache) > 5000:
                        f = file_cache.pop(sample(file_cache.keys(), 1)[0])
                        f.close()
