"""Minimal local Spark smoke for campaign temporary storage; all data is invented."""
import os
from pathlib import Path
import shutil
import tempfile

import pytest
from pyspark import SparkContext

from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse.config import ConfigurationError
from tfm_lakehouse.spark import spark_session

pytestmark = pytest.mark.integration


@pytest.fixture
def campaign(tmp_path, monkeypatch):
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', tmp_path / 'scalability')
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    monkeypatch.delenv('TFM_REAL_CONFIG', raising=False)
    for name in ('PYSPARK_SUBMIT_ARGS', 'SPARK_SUBMIT_OPTS', 'JAVA_TOOL_OPTIONS',
                 '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'SPARK_LOCAL_DIRS',
                 'PYSPARK_GATEWAY_PORT', 'SPARK_HOME', 'SPARK_CONF_DIR'):
        monkeypatch.delenv(name, raising=False)
    return workspace.reserve_campaign('smoke', 'synthetic-owner')


def test_reject_existing_gateway_before_start(campaign, monkeypatch):
    monkeypatch.setattr(SparkContext, '_gateway', object())
    with pytest.raises(ConfigurationError, match='fresh JVM'):
        with spark_session(campaign=campaign):
            pytest.fail('foreign gateway accepted')


def test_reject_foreign_campaign_before_launch(campaign):
    workspace._AUTHORIZED.clear()
    with pytest.raises(workspace.WorkspaceError, match='authorized'):
        with spark_session(campaign=campaign):
            pytest.fail('unregistered campaign accepted')


def test_local_spark_uses_campaign_temporary_storage(campaign, monkeypatch):
    source = Path.home() / '.ivy2' / 'jars'
    names = ('io.delta_delta-spark_2.12-3.3.3.jar',
             'io.delta_delta-storage-3.3.3.jar', 'org.antlr_antlr4-runtime-4.9.3.jar')
    if not all((source / name).is_file() for name in names):
        pytest.skip('pre-existing local Delta jars unavailable')
    jars = campaign.root / 'ivy' / 'jars'
    jars.mkdir()
    for name in names:
        shutil.copyfile(source / name, jars / name)
    conf = campaign.root / 'spark-conf'
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    before = {key: os.environ.get(key) for key in ('TMPDIR', 'TMP', 'TEMP',
              'SPARK_LOCAL_DIRS', 'JAVA_TOOL_OPTIONS', 'PYSPARK_PYTHON')}
    old_temp = tempfile.tempdir
    with spark_session(master='local[1]', campaign=campaign) as spark:
        assert spark.range(3).repartition(2).count() == 3
        assert spark.sparkContext.getConf().get('spark.local.dir') == str(campaign.root / 'spark-local')
        assert spark.sparkContext._jvm.java.lang.System.getProperty('java.io.tmpdir') == str(campaign.root / 'temp')
        assert tempfile.gettempdir() == str(campaign.root / 'temp')
        worker = spark.sparkContext.parallelize([1], 1).map(
            lambda _: (os.environ.get('TMPDIR'), tempfile.gettempdir())).collect()
        assert worker == [(str(campaign.root / 'temp'), str(campaign.root / 'temp'))]
    assert {key: os.environ.get(key) for key in before} == before
    assert tempfile.tempdir == old_temp


@pytest.mark.parametrize('name', ['SPARK_CONF_DIR', 'SPARK_HOME'])
def test_external_spark_path_rejected_before_inspection(campaign, monkeypatch, name):
    monkeypatch.setenv(name, '/invented-protected/no-inspection')
    monkeypatch.setattr('tfm_lakehouse.spark._validate_launcher_settings',
                        lambda **_: pytest.fail('inspected launcher paths'))
    with pytest.raises(ConfigurationError, match='configuration paths'):
        with spark_session(campaign=campaign):
            pytest.fail('foreign configuration accepted')
