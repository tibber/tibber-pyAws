import json
import os
import logging

_LOGGER = logging.getLogger(__name__)


def get_instance_id():
    try:
        filename = os.environ.get("ECS_CONTAINER_METADATA_FILE")
        if filename is not None:
            _LOGGER.info("ECS_CONTAINER_METADATA_FILE %s", filename)
            with open(filename, encoding="utf-8") as file:
                metadata = json.load(file)
            _LOGGER.info("metadata %s", metadata)
            _id = metadata.get("ContainerID")
            if len(_id) > 12:
                return _id[-12:]
            return _id
    except Exception:  # pylint: disable=broad-except
        _LOGGER.error("Failed to get instance id", exc_info=True)
    return os.uname()[1].replace(".", "").replace("-", "")
