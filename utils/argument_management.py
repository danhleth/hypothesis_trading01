"""
Modified from https://github.com/PaddlePaddle/PaddleOCR/blob/release/2.4/tools/program.py
"""
import os
import sys
import platform
import yaml
import time
import shutil
import paddle
import paddle.distributed as dist
from tqdm import tqdm
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from ppocr.utils.stats import TrainingStats
from ppocr.utils.save_load import save_model
from ppocr.utils.utility import print_dict
from ppocr.utils.logging import get_logger
from ppocr.utils import profiler
from ppocr.data import build_dataloader
import numpy as np


class ArgsParser(ArgumentParser):
    def __init__(self):
        super(ArgsParser, self).__init__(
            formatter_class=RawDescriptionHelpFormatter)
        self.add_argument("-c", "--config", help="configuration file to use")
        self.add_argument(
            "-o", "--opt", nargs='+', help="set configuration options")
        self.add_argument(
            '-p',
            '--profiler_options',
            type=str,
            default=None,
            help='The option of profiler, which should be in format \"key1=value1;key2=value2;key3=value3\".'
        )

    def parse_args(self, argv=None):
        args = super(ArgsParser, self).parse_args(argv)
        assert args.config is not None, \
            "Please specify --config=configure_file_path."
        args.opt = self._parse_opt(args.opt)
        return args

    def _parse_opt(self, opts):
        config = {}
        if not opts:
            return config
        for s in opts:
            s = s.strip()
            k, v = s.split('=')
            config[k] = yaml.load(v, Loader=yaml.Loader)
        return config


class AttrDict(dict):
    """Single level attribute dict, NOT recursive"""

    def __init__(self, **kwargs):
        super(AttrDict, self).__init__()
        super(AttrDict, self).update(kwargs)

    def __getattr__(self, key):
        if key in self:
            return self[key]
        raise AttributeError("object has no attribute '{}'".format(key))


global_config = AttrDict()

default_config = {'Global': {'debug': False, }}


def load_config(file_path):
    """
    Load config from yml/yaml file.
    Args:
        file_path (str): Path of the config file to be loaded.
    Returns: global config
    """
    merge_config(default_config)
    _, ext = os.path.splitext(file_path)
    assert ext in ['.yml', '.yaml'], "only support yaml files for now"
    merge_config(yaml.load(open(file_path, 'rb'), Loader=yaml.Loader))
    return global_config


def merge_config(config):
    """
    Merge config into global config.
    Args:
        config (dict): Config to be merged.
    Returns: global config
    """
    for key, value in config.items():
        if "." not in key:
            if isinstance(value, dict) and key in global_config:
                global_config[key].update(value)
            else:
                global_config[key] = value
        else:
            sub_keys = key.split('.')
            assert (
                sub_keys[0] in global_config
            ), "the sub_keys can only be one of global_config: {}, but get: {}, please check your running command".format(
                global_config.keys(), sub_keys[0])
            cur = global_config[sub_keys[0]]
            for idx, sub_key in enumerate(sub_keys[1:]):
                if idx == len(sub_keys) - 2:
                    cur[sub_key] = value
                else:
                    cur = cur[sub_key]

