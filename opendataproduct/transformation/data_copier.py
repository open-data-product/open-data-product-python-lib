import os
import shutil

from opendataproduct.config.data_transformation_silver_loader import DataTransformation
from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def copy_data(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    clean=False,
    quiet=False,
):
    if data_transformation.input_ports:
        for input_port in data_transformation.input_ports:
            for file in input_port.files:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.source_file_name
                )
                target_file_path = os.path.join(
                    results_path, input_port.id, file.target_file_name
                )

                if os.path.exists(source_file_path):
                    if clean or not os.path.exists(target_file_path):
                        os.makedirs(
                            os.path.join(results_path, input_port.id), exist_ok=True
                        )
                        shutil.copyfile(source_file_path, target_file_path)
                        not quiet and print(
                            f"✓ Copy {os.path.basename(source_file_path)} to {os.path.basename(target_file_path)}"
                        )
                    else:
                        not quiet and print(
                            f"✓ Already exists {os.path.basename(target_file_path)}"
                        )
                else:
                    not quiet and print(
                        f"✗️ Error: Source file does not exist {os.path.basename(source_file_path)}"
                    )
