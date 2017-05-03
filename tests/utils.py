import logging
import os
import re
import shakedown
import subprocess


def _init_logging():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('dcos').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


_init_logging()
LOGGER = logging.getLogger(__name__)
SPARK_PACKAGE_NAME='spark'


def hdfs_enabled():
    return os.environ.get("HDFS_ENABLED") != "false"


def is_strict():
    return os.environ.get('SECURITY') == 'strict'


def run_tests(app_url, app_args, expected_output, args=[]):
    task_id = _submit_job(app_url, app_args, args)
    LOGGER.info('Waiting for task id={} to complete'.format(task_id))
    shakedown.wait_for_task_completion(task_id)
    log = _task_log(task_id)
    LOGGER.info("task log: {}".format(log))
    assert expected_output in log


def _submit_job(app_url, app_args, args=[]):
    if is_strict():
        args += ["--conf", 'spark.mesos.driverEnv.MESOS_MODULES=file:///opt/mesosphere/etc/mesos-scheduler-modules/dcos_authenticatee_module.json']
        args += ["--conf", 'spark.mesos.driverEnv.MESOS_AUTHENTICATEE=com_mesosphere_dcos_ClassicRPCAuthenticatee']
        args += ["--conf", 'spark.mesos.principal=service-acct']
    args_str = ' '.join(args + ["--conf", "spark.driver.memory=2g"])
    submit_args = ' '.join([args_str, app_url, app_args])
    cmd = 'dcos --log-level=DEBUG spark --verbose run --submit-args="{0}"'.format(submit_args)

    LOGGER.info("Running {}".format(cmd))
    stdout = subprocess.check_output(cmd, shell=True).decode('utf-8')

    LOGGER.info("stdout: {}".format(stdout))

    regex = r"Submission id: (\S+)"
    match = re.search(regex, stdout)
    return match.group(1)


def _task_log(task_id):
    cmd = "dcos task log --completed --lines=1000 {}".format(task_id)
    LOGGER.info("Running {}".format(cmd))
    stdout = subprocess.check_output(cmd, shell=True).decode('utf-8')
    return stdout
