"""Bootstrap without Node, including GitHub's vendor-action launcher binaries.

Python is necessary here: this must execute before the first Node process.
Only disposable GitHub-hosted runners may be changed. The runner's node24
directory is a launcher ABI name; its actual executable becomes Node 26.
"""
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
from urllib.request import urlopen
import zipfile

VERSION = '26.10.0'
# nodejs.org/dist/v26.10.0/SHASUMS256.txt, verified 2026-09-28.
ARCHIVES = {
    ('Linux', 'X64'): ('linux-x64.tar.xz', 'ca70e9e349de048b9522abb3adc05b3bd6f43c5ffd3ec57916c7da292f59f022'),
    ('Linux', 'ARM64'): ('linux-arm64.tar.xz', '7a6353f63eb3d04765004b4adf172616243e4522434635cb1d26288658b04ab5'),
    ('macOS', 'X64'): ('darwin-x64.tar.gz', 'ebbe9ab9b58ad6bb54390d6e2c862c1afa7d4475fb7e8ae8146acde211bf70df'),
    ('macOS', 'ARM64'): ('darwin-arm64.tar.gz', '751fdf7439f115d87ee2a8f3f18c065b6151852068e3e666ac60ac2996f75ac9'),
    ('Windows', 'X64'): ('win-x64.zip', '9fef7eca6743a6b910989cd8e78712376b394fcb9b6e1e9c44a0799a287f90c5'),
    ('Windows', 'ARM64'): ('win-arm64.zip', 'b778640d7271566bcaa9679912cdf0684c13e824c114e41a8b696fb14af7a7aa'),
}


def runner_root():
    """Locate our actual Worker ancestor, without guessing another runner's path."""
    pid = os.getppid()
    for _ in range(12):
        if os.name == 'nt':
            script = (f'Get-CimInstance Win32_Process -Filter "ProcessId={pid}" | '
                      'Select-Object ExecutablePath,ParentProcessId | ConvertTo-Json -Compress')
            info = json.loads(subprocess.check_output(['powershell', '-NoProfile', '-Command', script], text=True))
            executable, pid = info['ExecutablePath'], int(info['ParentProcessId'])
        else:
            line = subprocess.check_output(['ps', '-p', str(pid), '-o', 'ppid=', '-o', 'comm='], text=True).strip()
            parent, executable = line.split(None, 1)
            if Path('/proc').is_dir():
                executable = os.readlink(f'/proc/{pid}/exe')
            pid = int(parent)
        path = Path(executable or '')
        if path.name in {'Runner.Worker', 'Runner.Worker.exe'}:
            root = path.resolve().parent.parent
            if not (root / 'externals').is_dir():
                raise RuntimeError('Runner Worker has no adjacent externals directory')
            return root
        if pid <= 1:
            break
    raise RuntimeError('Cannot locate this job\'s Runner.Worker; refusing to modify host tools')


def verify_node(executable):
    actual = subprocess.check_output([str(executable), '--version'], text=True).strip()
    if actual != f'v{VERSION}':
        raise RuntimeError(f'{executable}: expected v{VERSION}, got {actual}')


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted':
        raise RuntimeError('Node runtime replacement is restricted to disposable GitHub-hosted runners')
    runner = runner_root()
    suffix, digest = ARCHIVES[(os.environ['RUNNER_OS'], os.environ['RUNNER_ARCH'])]
    filename = f'node-v{VERSION}-{suffix}'
    destination = Path(os.environ['RUNNER_TEMP']) / f'etchv-node-{VERSION}'
    destination.mkdir(exist_ok=True)
    with urlopen(f'https://nodejs.org/dist/v{VERSION}/{filename}', timeout=120) as response:
        data = response.read()
    if sha256(data).hexdigest() != digest:
        raise RuntimeError('Node distribution checksum mismatch')
    archive = destination / filename
    archive.write_bytes(data)
    if suffix.endswith('.zip'):
        with zipfile.ZipFile(archive) as package:
            package.extractall(destination)
    else:
        with tarfile.open(archive) as package:
            package.extractall(destination, filter='data')
    archive.unlink()
    distribution = destination / filename.removesuffix('.tar.xz').removesuffix('.tar.gz').removesuffix('.zip')
    binary_dir = distribution if os.name == 'nt' else distribution / 'bin'
    executable_name = 'node.exe' if os.name == 'nt' else 'node'
    source = binary_dir / executable_name
    verify_node(source)
    launchers = sorted((runner / 'externals').glob(f'node*/bin/{executable_name}'))
    if not launchers:
        raise RuntimeError('No vendor-action Node launchers found; refusing an unverified CI environment')
    for target in launchers:
        # Keep the paths required by the runner but remove the old executable.
        try:
            shutil.copy2(source, target)
        except PermissionError:
            if os.name == 'nt':
                raise
            subprocess.run(['sudo', '-n', 'cp', str(source), str(target)], check=True)
        verify_node(target)
        print(f'Vendor launcher {target}: v{VERSION}', flush=True)
    with open(os.environ['GITHUB_PATH'], 'a') as output:
        output.write(str(binary_dir) + '\n')
    print(f'Job runtime: {source} (v{VERSION}); {len(launchers)} vendor launchers verified', flush=True)


if __name__ == '__main__':
    main()
