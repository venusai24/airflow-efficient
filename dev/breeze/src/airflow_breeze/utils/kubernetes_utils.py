# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import itertools
import os
import random
import re
import shutil
import socket
import stat
import sys
import tarfile
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any, NamedTuple
from urllib import request

from airflow_breeze.global_constants import (
    ALLOWED_ARCHITECTURES,
    HELM_VERSION,
    KIND_VERSION,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.host_info_utils import Architecture, get_host_architecture, get_host_os
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH, BUILD_CACHE_PATH
from airflow_breeze.utils.run_utils import RunCommandResult, run_command
from airflow_breeze.utils.shared_options import get_dry_run

K8S_ENV_PATH = AIRFLOW_ROOT_PATH / ".venv"
K8S_CLUSTERS_PATH = BUILD_CACHE_PATH / ".k8s-clusters"
K8S_BIN_BASE_PATH = K8S_ENV_PATH / "bin"
KIND_BIN_PATH = K8S_BIN_BASE_PATH / "kind"
KUBECTL_BIN_PATH = K8S_BIN_BASE_PATH / "kubectl"
HELM_BIN_PATH = K8S_BIN_BASE_PATH / "helm"
PYTHON_BIN_PATH = K8S_BIN_BASE_PATH / "python"
SCRIPTS_CI_KUBERNETES_PATH = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "kubernetes"
PYPROJECT_TOML_AIRFLOW_CORE_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "pyproject.toml"
KUBERNETES_TEST_PATH = AIRFLOW_ROOT_PATH / "kubernetes-tests"
PYPROJECT_TOML_KUBERNETES_TEST_PATH = KUBERNETES_TEST_PATH / "pyproject.toml"
CACHED_K8S_DEPS_HASH_PATH = K8S_ENV_PATH / "k8s_deps_hash.txt"
CHART_PATH = AIRFLOW_ROOT_PATH / "chart"

# In case of parallel runs those ports will be quickly allocated by multiple threads and closed, which
# might mean that the port will be re-bound by parallel running thread. That's why we do not close the
# socket here - we return it to the caller and only close the socket just before creating the cluster
# we also add them to  the "used set" so even if another thread will get between closing the socket
# and creating the cluster they will not reuse it and quickly close it

USED_SOCKETS: set[int] = set()


def get_kind_cluster_name(python: str, kubernetes_version: str) -> str:
    return f"airflow-python-{python}-{kubernetes_version}"


def get_kubectl_cluster_name(python: str, kubernetes_version: str) -> str:
    return f"kind-{get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)}"


def get_config_folder(python: str, kubernetes_version: str) -> Path:
    return K8S_CLUSTERS_PATH / get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)


def get_kubeconfig_file(python: str, kubernetes_version: str) -> Path:
    return get_config_folder(python=python, kubernetes_version=kubernetes_version) / ".kubeconfig"


def get_kind_cluster_config_path(python: str, kubernetes_version: str) -> Path:
    return get_config_folder(python=python, kubernetes_version=kubernetes_version) / ".kindconfig.yaml"


def get_architecture_string_for_urls() -> str:
    architecture, machine = get_host_architecture()
    if architecture == Architecture.X86_64:
        return "amd64"
    if architecture == Architecture.ARM:
        return "arm64"
    msg = f"The architecture {architecture} is not supported when downloading kubernetes tools!"
    raise SystemExit(msg)


def _download_with_retries(num_tries, path, tool, url):
    while num_tries:
        try:
            request.urlretrieve(url, str(path))
            st = os.stat(str(path))
            os.chmod(str(path), st.st_mode | stat.S_IEXEC)
            break
        except OSError as e:
            num_tries = num_tries - 1
            if num_tries == 0:
                get_console().print(f"[error]Failing on max retries. Error while downloading {tool}: {e}")
                sys.exit(1)
            get_console().print(
                f"[warning]Retrying: {num_tries} retries  left on error while downloading {tool} tool: {e}"
            )


def _download_tool_if_needed(
    tool: str,
    version: str,
    url: str,
    version_flag: list[str],
    version_pattern: str,
    path: Path,
    uncompress_file: str | None = None,
):
    expected_version = version.replace("v", "")
    try:
        result = run_command(
            [path.as_posix(), *version_flag],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and not get_dry_run():
            match = re.search(version_pattern, result.stdout)
            if not match:
                get_console().print(
                    f"[info]No regexp match for version check in `{tool}` tool output "
                    f"{version_pattern} in:[/]\n{result.stdout}\n"
                    f"[info]Downloading {expected_version}."
                )
            else:
                current_version = match.group(1)
                if current_version == expected_version:
                    get_console().print(
                        f"[success]Good version of {tool} installed: {expected_version} in "
                        f"{K8S_BIN_BASE_PATH}"
                    )
                    return
                get_console().print(
                    f"[info]Currently installed `{tool}` tool version: {current_version}. "
                    f"Downloading {expected_version}."
                )
        else:
            get_console().print(
                f"[warning]The version check of `{tool}` tool returned "
                f"{result.returncode} error. Downloading {expected_version} version."
            )
            get_console().print(result.stdout)
            get_console().print(result.stderr)
    except FileNotFoundError:
        get_console().print(
            f"[info]The `{tool}` tool is not downloaded yet. Downloading {expected_version} version."
        )
    except OSError as e:
        get_console().print(
            f"[info]Error when running `{tool}`: {e}. Removing and downloading {expected_version} version."
        )
        path.unlink(missing_ok=True)
    get_console().print(f"[info]Downloading from:[/] {url}")
    if get_dry_run():
        return
    path.unlink(missing_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    num_tries = 4
    if not uncompress_file:
        _download_with_retries(num_tries, path, tool, url)
    else:
        with tempfile.NamedTemporaryFile(delete=True) as f:
            _download_with_retries(num_tries, Path(f.name), tool, url)
            tgz_file = tarfile.open(f.name)
            get_console().print(f"[info]Extracting the {uncompress_file} to {path.parent}[/]")
            with tempfile.TemporaryDirectory() as d:
                tgz_file.extract(uncompress_file, str(d))
                target_file = Path(d) / uncompress_file
                get_console().print(f"[info]Moving the {target_file.name} to {path}[/]")
                shutil.move(str(target_file), str(path))


def _download_kind_if_needed():
    _download_tool_if_needed(
        tool="kind",
        version=KIND_VERSION,
        version_flag=["--version"],
        version_pattern=r".*[^\d].*(\d+\.\d+\.\d+)[^\d]*.*$",
        url=f"https://github.com/kubernetes-sigs/kind/releases/download/"
        f"{KIND_VERSION}/kind-{get_host_os()}-{get_architecture_string_for_urls()}",
        path=KIND_BIN_PATH,
    )


def _download_kubectl_if_needed():
    import requests

    kubectl_version = requests.get(
        "https://storage.googleapis.com/kubernetes-release/release/stable.txt"
    ).text
    _download_tool_if_needed(
        tool="kubectl",
        version=kubectl_version,
        version_pattern=r".*gitVersion:[^\d].*(\d+\.\d+\.\d+)[^\d]*",
        version_flag=["version", "--client", "--output", "yaml"],
        url=f"https://storage.googleapis.com/kubernetes-release/release/"
        f"{kubectl_version}/bin/{get_host_os()}/{get_architecture_string_for_urls()}/kubectl",
        path=KUBECTL_BIN_PATH,
    )


def _download_helm_if_needed():
    _download_tool_if_needed(
        tool="helm",
        version=HELM_VERSION,
        version_pattern=r"v(\d+\.\d+\.\d+)$",
        version_flag=["version", "--template", "{{.Version}}"],
        url=f"https://get.helm.sh/"
        f"helm-{HELM_VERSION}-{get_host_os()}-{get_architecture_string_for_urls()}.tar.gz",
        path=HELM_BIN_PATH,
        uncompress_file=f"{get_host_os()}-{get_architecture_string_for_urls()}/helm",
    )


def _check_architecture_supported():
    architecture, machine = get_host_architecture()
    if architecture not in ALLOWED_ARCHITECTURES:
        get_console().print(
            f"[error]The {architecture} is not one "
            f"of the supported: {ALLOWED_ARCHITECTURES}. The original machine: {machine}"
        )
        sys.exit(1)


def make_sure_helm_installed():
    K8S_CLUSTERS_PATH.mkdir(parents=True, exist_ok=True)
    _check_architecture_supported()
    _download_helm_if_needed()


def make_sure_kubernetes_tools_are_installed():
    K8S_CLUSTERS_PATH.mkdir(parents=True, exist_ok=True)
    _check_architecture_supported()
    _download_kind_if_needed()
    _download_kubectl_if_needed()
    _download_helm_if_needed()
    new_env = os.environ.copy()
    new_env["PATH"] = str(K8S_BIN_BASE_PATH) + os.pathsep + new_env["PATH"]
    result = run_command(
        ["helm", "repo", "list"],
        check=False,
        capture_output=True,
        env=new_env,
        text=True,
    )
    if get_dry_run() or result.returncode == 0 and "stable" in result.stdout:
        get_console().print("[info]Stable repo is already added")
    else:
        get_console().print("[info]Adding stable repo")
        run_command(
            ["helm", "repo", "add", "stable", "https://charts.helm.sh/stable"],
            check=False,
            env=new_env,
        )


def sync_virtualenv(force_venv_setup: bool) -> RunCommandResult:
    uv_sync_cmd = ["uv", "sync"]
    if force_venv_setup:
        uv_sync_cmd.append("--reinstall")
    if os.environ.get("CI", "false") == "true":
        uv_sync_cmd.append("--no-progress")
    return run_command(uv_sync_cmd, cwd=KUBERNETES_TEST_PATH, check=False)


def run_command_with_k8s_env(
    cmd: list[str] | str,
    python: str,
    kubernetes_version: str,
    executor: str | None = None,
    title: str | None = None,
    *,
    check: bool = True,
    no_output_dump_on_exception: bool = False,
    output: Output | None = None,
    input: str | None = None,
    **kwargs,
) -> RunCommandResult:
    return run_command(
        cmd,
        title,
        env=get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor),
        check=check,
        no_output_dump_on_exception=no_output_dump_on_exception,
        input=input,
        output=output,
        **kwargs,
    )


def get_k8s_env(python: str, kubernetes_version: str, executor: str | None = None) -> dict[str, str]:
    from packaging.version import Version

    new_env = os.environ.copy()
    new_env["PATH"] = str(K8S_BIN_BASE_PATH) + os.pathsep + new_env["PATH"]
    new_env["KUBECONFIG"] = str(get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version))
    new_env["KINDCONFIG"] = str(
        get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)
    )
    _, api_server_port = get_kubernetes_port_numbers(python=python, kubernetes_version=kubernetes_version)
    new_env["CLUSTER_FORWARDED_PORT"] = str(api_server_port)
    kubectl_cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    if executor:
        new_env["PS1"] = f"({kubectl_cluster_name}:{executor})> "
        new_env["EXECUTOR"] = executor
    use_flask_appbuilder = Version(python) < Version("3.13")
    if use_flask_appbuilder:
        new_env["USE_FAB_AUTH_MANAGER"] = "true"
    else:
        new_env["USE_FAB_AUTH_MANAGER"] = "false"
    return new_env


START_PORT_RANGE = 10000
END_PORT_RANGE = 49000


def _get_free_port() -> int:
    while True:
        port = random.randrange(START_PORT_RANGE, END_PORT_RANGE)
        if port in USED_SOCKETS:
            continue
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", port))
            s.close()
        except OSError:
            continue
        finally:
            s.close()
        USED_SOCKETS.add(port)
        return port


def _get_kind_cluster_config_content(python: str, kubernetes_version: str) -> dict[str, Any] | None:
    config_path = get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)
    if not config_path.exists():
        get_console().print(f"[warning]The kind cluster config file {config_path} does not exist!")
        return None

    import yaml

    return yaml.safe_load(config_path.read_text())


def set_random_cluster_ports(python: str, kubernetes_version: str, output: Output | None) -> None:
    """
    Creates cluster config file and returns sockets keeping the ports bound.
    The sockets should be closed just before creating the cluster.
    """
    forwarded_port_number = _get_free_port()
    k8s_api_server_port = _get_free_port()
    get_console(output=output).print(
        f"[info]Random ports: K8S API: {k8s_api_server_port}, API Server: {forwarded_port_number}"
    )
    cluster_conf_path = get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)
    config = (
        (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "kubernetes" / "kind-cluster-conf.yaml")
        .read_text()
        .replace("{{FORWARDED_PORT_NUMBER}}", str(forwarded_port_number))
        .replace("{{API_SERVER_PORT}}", str(k8s_api_server_port))
    )
    cluster_conf_path.write_text(config)
    get_console(output=output).print(f"[info]Config created in {cluster_conf_path}:\n")
    get_console(output=output).print(config)
    get_console(output=output).print("\n")


def get_kubernetes_port_numbers(python: str, kubernetes_version: str) -> tuple[int, int]:
    conf = _get_kind_cluster_config_content(python=python, kubernetes_version=kubernetes_version)
    if not conf:
        return 0, 0
    k8s_api_server_port = conf["networking"]["apiServerPort"]
    api_server_port = conf["nodes"][1]["extraPortMappings"][0]["hostPort"]
    return k8s_api_server_port, api_server_port


def _attempt_to_connect(port_number: int, output: Output | None, wait_seconds: int = 0) -> bool:
    import requests

    start_time = datetime.now(timezone.utc)
    sleep_seconds = 5
    for attempt in itertools.count(1):
        get_console(output=output).print(f"[info]Connecting to localhost:{port_number}. Num try: {attempt}")
        try:
            response = requests.get(f"http://localhost:{port_number}/api/v2/monitor/health")
        except ConnectionError:
            get_console(output=output).print(
                f"The api server is not yet ready at http://localhost:{port_number}/api/v2/monitor/health "
            )
        except Exception as e:
            get_console(output=output).print(f"[info]Error when connecting to localhost:{port_number} : {e}")
        else:
            if response.status_code == 200:
                get_console(output=output).print(
                    "[success]Established connection to api server at "
                    f"http://localhost:{port_number}/api/v2/monitor/health and it is healthy."
                )
                return True
            get_console(output=output).print(
                f"[warning]Error when connecting to localhost:{port_number} "
                f"{response.status_code}: {response.reason}"
            )
        current_time = datetime.now(timezone.utc)
        if current_time - start_time > timedelta(seconds=wait_seconds):
            if wait_seconds > 0:
                get_console(output=output).print(f"[error]More than {wait_seconds} passed. Exiting.")
            break
        get_console(output=output).print(f"Sleeping for {sleep_seconds} seconds.")
        sleep(sleep_seconds)
    return False


def print_cluster_urls(
    python: str, kubernetes_version: str, output: Output | None, wait_time_in_seconds: int = 0
):
    k8s_api_server_port, api_server_port = get_kubernetes_port_numbers(
        python=python, kubernetes_version=kubernetes_version
    )
    get_console(output=output).print(
        f"\n[info]Kubeconfig file in: {get_kubeconfig_file(python, kubernetes_version)}\n"
    )
    get_console(output=output).print(
        f"\n[info]KinD Cluster API server URL: [/]http://localhost:{k8s_api_server_port}"
    )
    if _attempt_to_connect(port_number=api_server_port, output=output, wait_seconds=wait_time_in_seconds):
        get_console(output=output).print(
            f"[info]Airflow API server URL: [/]http://localhost:{api_server_port} (admin/admin)\n"
        )
    else:
        get_console(output=output).print(
            f"\n[warning]Airflow API server is not available at port {api_server_port}. "
            f"Run `breeze k8s deploy-airflow --python {python} --kubernetes-version {kubernetes_version}` "
            "to (re)deploy airflow\n"
        )


class KubernetesPythonVersion(NamedTuple):
    kubernetes_version: str
    python_version: str


def _get_k8s_python_version(
    index: int, kubernetes_version_array: list[str], python_version_array: list[str]
) -> KubernetesPythonVersion:
    current_python = python_version_array[index % len(python_version_array)]
    current_kubernetes_version = kubernetes_version_array[index % len(kubernetes_version_array)]
    return KubernetesPythonVersion(
        kubernetes_version=current_kubernetes_version, python_version=current_python
    )


def get_kubernetes_python_combos(
    kubernetes_version_array, python_version_array
) -> tuple[list[str], list[str], list[KubernetesPythonVersion]]:
    num_tests = max(len(python_version_array), len(kubernetes_version_array))
    combos: list[KubernetesPythonVersion] = [
        _get_k8s_python_version(i, kubernetes_version_array, python_version_array) for i in range(num_tests)
    ]
    combo_titles = [
        get_kind_cluster_name(python=combo.python_version, kubernetes_version=combo.kubernetes_version)
        for combo in combos
    ]
    short_combo_titles = [combo[len("airflow-python-") :] for combo in combo_titles]
    return combo_titles, short_combo_titles, combos
