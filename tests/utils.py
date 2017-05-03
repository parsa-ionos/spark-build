import dcos.config
import dcos.http
import dcos.package

import logging
import os
import re
import shakedown
import subprocess
import urllib


def _init_logging():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('dcos').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


_init_logging()
LOGGER = logging.getLogger(__name__)
DEFAULT_HDFS_TASK_COUNT=10
HDFS_PACKAGE_NAME='beta-hdfs'
HDFS_SERVICE_NAME='hdfs'
SPARK_PACKAGE_NAME='spark'


def hdfs_enabled():
    return os.environ.get("HDFS_ENABLED") != "false"


def is_strict():
    return os.environ.get('SECURITY') == 'strict'


def require_hdfs():
    LOGGER.info("Ensuring HDFS is installed.")

    _require_package(HDFS_PACKAGE_NAME, _get_hdfs_options())
    _wait_for_hdfs()


def require_spark():
    LOGGER.info("Ensuring Spark is installed.")

    _require_package(SPARK_PACKAGE_NAME, _get_spark_options())
    _wait_for_spark()
    _require_spark_cli()


# This should be in shakedown (DCOS_OSS-679)
def _require_package(pkg_name, options = {}):
    pkg_manager = dcos.package.get_package_manager()
    installed_pkgs = dcos.package.installed_packages(pkg_manager, None, None, False)

    if any(pkg['name'] == pkg_name for pkg in installed_pkgs):
        LOGGER.info("Package {} already installed.".format(pkg_name))
    else:
        LOGGER.info("Installing package {}".format(pkg_name))
        shakedown.install_package(
            pkg_name,
            options_json=options,
            wait_for_completion=True)


def _wait_for_spark():
    def pred():
        dcos_url = dcos.config.get_config_val("core.dcos_url")
        spark_url = urllib.parse.urljoin(dcos_url, "/service/spark")
        status_code = dcos.http.get(spark_url).status_code
        return status_code == 200

    shakedown.wait_for(pred)


def _require_spark_cli():
    LOGGER.info("Ensuring Spark CLI is installed.")
    installed_subcommands = dcos.package.installed_subcommands()
    if any(sub.name == SPARK_PACKAGE_NAME for sub in installed_subcommands):
        LOGGER.info("Spark CLI already installed.")
    else:
        LOGGER.info("Installing Spark CLI.")
        shakedown.run_dcos_command('package install --cli {}'.format(
            SPARK_PACKAGE_NAME))


def _get_hdfs_options():
    if is_strict():
        options = {'service': {'principal': 'service-acct', 'secret_name': 'secret'}}
    else:
        options = {"service": {}}

    options["service"]["beta-optin"] = True
    return options


def _wait_for_hdfs():
    shakedown.wait_for(_is_hdfs_ready, ignore_exceptions=False, timeout_seconds=900)


def _is_hdfs_ready(expected_tasks = DEFAULT_HDFS_TASK_COUNT):
    running_tasks = [t for t in shakedown.get_service_tasks(HDFS_SERVICE_NAME) \
                     if t['state'] == 'TASK_RUNNING']
    return len(running_tasks) >= expected_tasks


def _get_spark_options():
    if hdfs_enabled():
        options = {"hdfs":
                       {"config-url":
                            "http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints"}}
    else:
        options = {}

    if is_strict():
        options.update({'service':
                            {"principal": "service-acct"},
                        "security":
                            {"mesos":
                                 {"authentication":
                                      {"secret_name": "secret"}}}})

    return options


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
