import os
import getopt
import logging
import multiprocessing
import sys
import time
import traceback
import functools

import benchmark.benchmark_config as BenchmarkConfig
import benchmark.auth as auth

LOG = logging.getLogger(__name__)


def run_job(job, params):
    return list(map((lambda job: job.run(params)), job['actions']))


def main(argv):

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    )

    LOG.info("Current working directory: %s" % os.getcwd())

    try:
        parsed_args = parse_config_args(argv, ['config='])
        config_path = parsed_args['--config']
        if not config_path:
            raise RuntimeError("Encountered null or empty config path, exiting; config_path: %s, args: %s",
                               config_path, str(argv))
        LOG.info("Loading config.yml from: %s", config_path)
        jobs = BenchmarkConfig.parse_benchmark_config(config_path)
        LOG.info("Parsed %s %s", str(len(jobs)), "job" if len(jobs) == 1 else "jobs")
        pool = multiprocessing.Pool(processes=4)
        params = auth.register_user(email='benchmark@mit.edu', username='benchmark', password='benchmark')
        start_time = time.time()
        async = pool.map_async(functools.partial(run_job, params=params), jobs)
        async.wait(timeout=300)
        end_time = time.time()
        LOG.info("Finished all benchmark jobs, took: {0}".format(str(end_time - start_time)))
    except getopt.GetoptError as error:
        LOG.error("Must supply config YAML path as first argument: %s", error)
        traceback.format_exc()
    except Exception as exception:
        LOG.error("Caught exception: %s", exception)
        traceback.format_exc()
    finally:
        LOG.info("Shutting down")
        sys.exit(0)


def parse_config_args(argv, long_opts):
    parsed_args = {}
    options, _ = getopt.getopt(argv, shortopts='', longopts=long_opts)
    for option in options:
        if option[0] not in long_opts:
            if option[1]:
                if option[0] not in parsed_args.keys():
                    LOG.info("Found path: %s", option[1])
                    parsed_args[option[0]] = option[1]
                else:
                    LOG.warning("Found duplicate, rejecting: {'%s': '%s'}, current parsed_args: %s",
                                option[0], option[1], str(parsed_args))
            else:
                raise RuntimeError("Found config key: %s, but no value: %s", option[0], option[1])
        else:
            LOG.warning("Ignoring unused option: %s", str(option))
    return parsed_args


if __name__ == "__main__":
    main(sys.argv[1:])