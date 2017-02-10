import os
import importlib
import logging

import yaml

LOG = logging.getLogger(__name__)


def parse_benchmark_config(path_to_yaml):
    """ Method benchmark action yaml """
    with open(os.path.expanduser(path_to_yaml), 'r') as stream:
        try:
            parsed_yaml = yaml.load(stream)
            jobs = []
            for job in parsed_yaml['jobs']:
                job_args = {k: v for k, v in job.items()}
                job_args['actions'] = _build_actions(job['actions'], parsed_yaml['dive_url'])
                jobs.append(job_args)
            return jobs
        except yaml.YAMLError as exc:
            LOG.error(exc)


def _build_actions(yaml_actions, dive_url):
    """ Constructs job for a single YAML entry """
    actions = []
    for action in yaml_actions:
        action_class = getattr(importlib.import_module(action['action_module']), action['action_class_name'])
        filtered_action_args = {k: v for k, v in action.items() if k in action_class.ACTION_ARG_WHITELIST}
        filtered_action_args['dive_url'] = dive_url
        LOG.info("Building action with args: %s", str(filtered_action_args))
        if not _is_valid_args(filtered_action_args, action_class.ACTION_ARG_WHITELIST):
            raise Exception("Invalid arguments for action")
        actions.append(action_class(**filtered_action_args))
    return actions


def _is_valid_args(args, arg_whitelist):
    for required_arg in arg_whitelist:
        if required_arg not in args:
            LOG.error("Missing required argument: %s, args: %s, required args: %s", required_arg, args, arg_whitelist)
            return False
    return True