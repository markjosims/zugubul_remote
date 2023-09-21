#!/usr/bin/env python3
"""fabfile"""
import os
import sys
from fabric import Connection
from paramiko.ssh_exception import PasswordRequiredException
from typing import Sequence, Optional
from pathlib import Path
from gooey import Gooey, GooeyParser

def run_script_on_server(
        argv: Sequence[str],
        in_files: Sequence[str],
        out_files: Sequence[str],
        server: str,
        server_python: str,
        passphrase: str,
    ) -> int:
    with connect(server, passphrase) as c:
        # replace local fps w server fps in arg str and put to server
        # TODO: dynamically check if input file is already present
        print(f'Uploading input files to server {server}...')
        server_dir = Path('/tmp/annotate/')
        c.run(f'mkdir -p {server_dir}')
        for local_fp in in_files:
            filename = Path(local_fp).name
            server_fp = server_dir/filename
            
            print(f'Putting {local_fp} to {server_fp}')
            #c.put(str(local_fp), str(server_fp))
            argv = [arg if arg!=local_fp else str(server_fp) for arg in argv]

        # replace local fps with server fp but don't put to server
        server_out_files = []
        for local_fp in out_files:
            filename = Path(local_fp).name
            server_fp = server_dir/filename
            server_out_files.append(server_fp)
            argv = [arg if arg!=local_fp else str(server_fp) for arg in argv]

        print('Executing command on server...')
        argv = [server_python, '-m', 'zugubul.main'] + argv[1:]
        arg_str = make_arg_str(argv)
        c.run(arg_str)

        print('Downloading output files from server...')
        for local_fp, server_fp in zip(out_files, server_out_files):
            c.get(str(server_fp), str(local_fp))
            print('Output filed saved to', local_fp)

def make_arg_str(argv: Sequence[str]) -> str:
    """
    Wrap any arguments broken by whitespace with quotes,
    then join arguments into str and return.
    """
    has_whitespace = lambda s: any(c.isspace() for c in s)
    wrap_if_whitespace = lambda s: '"'+s+'"' if has_whitespace(s) else s
    argv = [wrap_if_whitespace(s) for s in argv]
    arg_str = ' '.join(argv)
    return arg_str


def connect(address: str, passphrase: Optional[str]=None) -> Connection:
    connect_kwargs = {
        'passphrase': passphrase or os.getenv('SSH_PASSPHRASE')
    }
    r = Connection(address, connect_kwargs=connect_kwargs)
    del connect_kwargs
    del passphrase
    return r

def is_valid_file(parser: GooeyParser, arg: str) -> str:
    """
    Return error if filepath not found, return filepath otherwise.
    """
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        return arg

def init_annotate_parser(annotate_parser: GooeyParser) -> None:
    add_arg = annotate_parser.add_argument
    add_arg(
        "WAV_FILE",
        type=lambda x: is_valid_file(annotate_parser, x),
        help='Path to .wav file to run inference on.',
        widget='FileChooser'
    )
    add_arg(
        "OUT",
        help='Path to .eaf file to save annotations to.',
        widget='DirChooser'
    )
    add_arg(
        "PASSWORD",
        help="Password to log in to server.",
        widget="PasswordField",
    )
    add_arg(
        "LID_URL",
        help="Path to HuggingFace model to use for language identification.",
        default='markjosims/wav2vec2-large-mms-1b-tira-lid'
    )
    add_arg(
        "ASR_URL",
        help="Path to HuggingFace model to use for automatic speech recognition.",
        default='markjosims/wav2vec2-large-xls-r-300m-tira-colab'
    )
    add_arg(
        "LANG",
        help="ISO code for target language to annotate (TIC is the ISO code for Tira).",
        default='TIC'
    )
    add_arg(
        "--inference_method",
        "-im",
        choices=['local', 'api', 'try_api'],
        default='local',
        help='Method for running inference. If local, download model if not already downloaded and '\
            +'run pipeline. If api, use HuggingFace inference API. If try_api, call HuggingFace API '\
            +'but run locally if API returns error.'        
    )
    add_arg(
        '-t',
        '--tier',
        default='IPA Transcription',
        help="Add ASR annotations to given tier. By default `default-lt`."
    )
    add_arg(
        '--template',
        type=lambda x: is_valid_file(annotate_parser, x),
        help='Template .etf file for generating output .eafs.',
        widget='FileChooser'
    )

@Gooey
def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = GooeyParser()
    init_annotate_parser(parser)
    args = parser.parse_args(argv)

    in_files = [ args.WAV_FILE]
    if args.template:
        in_files.append(args.template)
    out_name = os.path.basename(args.WAV_FILE)
    out_path = os.path.join(args.OUT, out_name)
    out_files = [out_path]

    argv = sys.argv
    # remove PASSWORD arg
    argv.remove(args.PASSWORD)

    # move OUT arg to after LANG
    argv.remove(args.OUT)
    argv[5] = args.OUT

    run_script_on_server(
        sys.argv,
        in_files,
        out_files,
        server='mjsimmons@grice.ucsd.edu',
        server_python='zugubul/.venv/bin/python',
        passphrase=args.PASSWORD
    )

    return 0

if __name__ == '__main__':
    main()