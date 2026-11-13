import argparse
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RUST = {
    'scc': 'FunnelQ_Columnar_Count_Experiment',
    'nft-funnelq': 'native/nft/funnelq',
    'nft-hgmatch': 'native/nft/hgmatch',
    'defi-funnelq': 'FunnelQ_code_release/defi/systems/hvceq/rust_kernel',
    'defi-hgmatch-sametx': 'native/defi/hgmatch/same_tx',
    'defi-hgmatch-episode': 'native/defi/hgmatch/episode',
    'defi-vchain': 'experiments/defi_native_verification/revision_v4/vchain',
    'nft-vchain': 'VChainBaseline/q15_server',
    'yo-hgmatch': 'native/yo/hgmatch',
    'yo-vchain': 'native/yo/vchain',
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('component', choices=[*RUST, 'nft-mbft', 'defi-mbft', 'yo-mbft', 'defi-mbft-verify', 'yo-mbft-verify'])
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--jobs', type=int, default=4)
    args = parser.parse_args()
    if args.component in RUST:
        manifest = ROOT / RUST[args.component] / 'Cargo.toml'
        command = ['cargo', 'check' if args.check else 'build', '--locked',
                   '--manifest-path', str(manifest), '-j', str(args.jobs)]
        if not args.check:
            command.append('--release')
        if args.offline:
            command.append('--offline')
        subprocess.run(command, cwd=ROOT, check=True)
    elif args.component.endswith('-verify'):
        core = ROOT / 'experiments/defi_native_verification/revision_v4/mbft'
        adapter = ROOT / ('experiments/defi_native_verification/revision_v4/mbft_bridge.cpp'
                          if args.component == 'defi-mbft-verify'
                          else 'experiments/yo_verification/mbft_bridge.cpp')
        output = ROOT / 'build' / args.component
        output.parent.mkdir(exist_ok=True)
        command = ['g++', '-O3', '-std=c++17', '-I' + str(core / 'include'), str(adapter)]
        command += [str(core / 'src' / (name + '.cpp')) for name in
                    ['adapt', 'persist', 'bf', 'blockchain', 'hash', 'merkle', 'query', 'transaction']]
        subprocess.run(command + ['-o', str(output)], check=True)
    else:
        source = ROOT / 'native' / args.component.split('-')[0] / 'mbft'
        build = ROOT / 'build' / args.component
        configure = ['cmake', '-S', str(source), '-B', str(build), '-DCMAKE_BUILD_TYPE=Release']
        target = 'nft_resident' if args.component == 'nft-mbft' else 'mbft_resident'
        if args.component != 'nft-mbft':
            configure.append('-DMBFT_SOURCE=' + str(ROOT / 'FunnelQ_code_release/defi/MBFT-Adapt_MergeBloom_QueryOnly_v3_source'))
        subprocess.run(configure, check=True)
        subprocess.run(['cmake', '--build', str(build), '--config', 'Release',
                        '--target', target, '-j', str(args.jobs)], check=True)

if __name__ == '__main__':
    main()
