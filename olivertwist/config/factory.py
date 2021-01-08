# -*- coding: utf-8 -*-
"""Document __init__.py here.

Copyright (C) 2020, Auto Trader UK
Created 22. Dec 2020 19:36

"""
from collections import Counter
from pathlib import Path
from typing import Union

import yaml

try:
    from yaml import CSafeLoader as YamlLoader
except ImportError:
    from yaml import SafeLoader as YamlLoader

from dataclasses_jsonschema import ValidationError

from olivertwist.config.model import Config


class InvalidConfigError(Exception):
    """Thrown if an invalid configuration file is supplied."""


class DuplicateEntryError(InvalidConfigError):
    """Duplicate sections were present in the supplied config."""


DEFAULT_CONFIG_FILE_PATH = "./olivertwist.yml"


class ConfigFactory:
    @classmethod
    def create_config_from_path(cls, path: Union[Path, str]) -> Config:
        path = path or DEFAULT_CONFIG_FILE_PATH
        try:
            return cls.__validate(cls.__parse(path))
        except FileNotFoundError:
            return Config(universal=[])

    @classmethod
    def __parse(cls, config_file_path) -> Config:
        try:
            with open(config_file_path, "rb") as handle:
                yaml_config_dict = yaml.load(
                    handle.read().decode("utf-8"), Loader=YamlLoader
                )
                return Config.from_dict(yaml_config_dict)
        except ValidationError as e:
            raise InvalidConfigError(e)

    @classmethod
    def __validate(cls, config: Config) -> Config:
        entry_counts = Counter(rule.id for rule in config.universal)
        duplicates = [id_ for id_, count in entry_counts.items() if count > 1]
        if duplicates:
            raise DuplicateEntryError(", ".join(duplicates))

        return config
