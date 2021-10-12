import WDL
import logging
import json
import sys
import os
import platform
import tempfile
import json
import logging
import asyncio
import atexit
import textwrap
from shlex import quote as shellquote
from argparse import ArgumentParser, Action, SUPPRESS, RawDescriptionHelpFormatter
from contextlib import ExitStack
import argcomplete
import argparse
import base64
import copy
import datetime
import errno
import functools
import json
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import textwrap
import urllib
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Text,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib import parse as urlparse

import cwltool.builder
import cwltool.command_line_tool
import cwltool.context
import cwltool.errors
import cwltool.expression
import cwltool.load_tool
import cwltool.main
import cwltool.provenance
import cwltool.resolver
import cwltool.stdfsaccess
import schema_salad.ref_resolver
from cwltool.loghandler import _logger as cwllogger
from cwltool.loghandler import defaultStreamHandler
from cwltool.mpi import MpiConfig
from cwltool.mutation import MutationManager
from cwltool.pathmapper import MapperEnt, PathMapper
from cwltool.process import (
    Process,
    add_sizes,
    compute_checksums,
    fill_in_defaults,
    shortname,
)
from cwltool.secrets import SecretStore
from cwltool.software_requirements import (
    DependenciesConfiguration,
    get_container_from_software_requirements,
)
from cwltool.utils import (
    CWLObjectType,
    CWLOutputType,
    adjustDirObjs,
    adjustFileObjs,
    aslist,
    get_listing,
    normalizeFilesDirs,
    visit_class,
    downloadHttpFile,
)
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from schema_salad import validate
from schema_salad.exceptions import ValidationException
from schema_salad.avro.schema import Names
from schema_salad.sourceline import SourceLine, cmap
from threading import Thread

from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM
from options import addOptions, Config
from toil.cwl.utils import (
    download_structure,
    visit_top_cwl_class,
    visit_cwl_class_and_reduce,
)
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.jobStores.fileJobStore import FileJobStore
from toil.lib.threading import ExceptionalThread
from toil.version import baseVersion

from WDL.runtime.config import Loader
from WDL.CLI import (fill_common,
                     fill_check_subparser,
                     fill_run_subparser,
                     fill_configure_subparser,
                     fill_run_self_test_subparser,
                     fill_localize_subparser,
                     check,
                     runner,
                     run_self_test,
                     localize,
                     configure)

logger = logging.getLogger(__name__)


class WDLJob(Job):
    def __init__(self, job, inputs, options):
        super(WDLJob, self).__init__(
            cores=1,
            memory=1024 * 1024 * 1024,
            disk=1024 * 1024 * 1024,
            unitName='change_this',
            displayName='whatever'
        )
        self.job = job
        self.inputs = inputs
        self.options = options

    def run(self, file_store: AbstractFileStore) -> Any:
        logger.debug('Running a job.')

        self.job = resolve_dict_w_promises(self.job, file_store)

        # if self.conditional.is_false(cwljob):
        #     return self.conditional.skipped_outputs()

        fill_in_defaults(
            self.step_inputs, cwljob, self.runtime_context.make_fs_access("")
        )
        required_env_vars = self.populate_env_vars(cwljob)

        output, status = ToilSingleJobExecutor().execute()
        # ended_at = datetime.datetime.now()  # noqa F841
        # if status != "success":
        #     raise cwltool.errors.WorkflowException(status)

        # Upload all the Files and set their and the Directories' locations, if needed.
        import_files(
            file_import_function,
            fs_access,
            index,
            existing,
            output,
            bypass_file_store=runtime_context.bypass_file_store,
        )

        return output


def task(wdl: str, inputs: str, **kwargs):
    cfg = Loader(logging.getLogger('test'), [])
    with open(wdl, 'r') as f:
        wdl_content = f.read()
    with open(inputs, 'r') as f:
        inputs_dict = json.load(f)
    doc = WDL.parse_document(wdl_content, version='draft-2')
    doc.typecheck()

    for task in doc.tasks:
        print(task.available_inputs)
        print([i.name for i in task.available_inputs])
        print(task.required_inputs)
        print([i.name for i in task.required_inputs])
        inputs_dict = {'inputFile': '/home/quokka/git/toil/src/toil/test/wdl/md5sum/md5sum.input'}
        inputs = WDL.values_from_json(inputs_dict, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
        rundir, outputs = WDL.runtime.run_local_task(cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()),
                                                     run_dir='/home/quokka/git/miniwdl/run_dir', **kwargs)
        return WDL.values_to_json(outputs)


def main(args=None):
    args = args if args is not None else sys.argv[1:]
    sys.setrecursionlimit(1_000_000)  # permit as much call stack depth as OS can give us

    parser = argparse.ArgumentParser('toil-wdl-runner')

    config = Config()
    addOptions(parser, config, jobstore_is_optional=True)
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = "command"
    fill_common(fill_check_subparser(subparsers))
    fill_configure_subparser(subparsers)
    fill_common(fill_run_subparser(subparsers))
    argcomplete.autocomplete(parser)

    replace_COLUMNS = os.environ.get("COLUMNS", None)
    os.environ["COLUMNS"] = "100"  # make help descriptions wider
    args = parser.parse_args(args)

    if replace_COLUMNS is not None:
        os.environ["COLUMNS"] = replace_COLUMNS
    else:
        del os.environ["COLUMNS"]

    try:
        if args.command == "check":
            check(**vars(args))
        elif args.command == "run":
            runner(**vars(args))
        elif args.command == "run_self_test":
            run_self_test(**vars(args))
        elif args.command == "localize":
            localize(**vars(args))
        elif args.command == "configure":
            configure(**vars(args))
        else:
            assert False
    except (
        WDL.Error.SyntaxError,
        WDL.Error.ImportError,
        WDL.Error.ValidationError,
        WDL.Error.MultipleValidationErrors,
    ) as exn:
        global quant_warning
        if args.check_quant and quant_warning:
            print(
                "* Hint: for compatibility with older existing WDL code, try setting --no-quant-check to relax "
                "quantifier validation rules.",
                file=sys.stderr,
            )
        if args.debug:
            raise exn
        sys.exit(2)
    sys.exit(0)


if __name__ == '__main__':
    main()
