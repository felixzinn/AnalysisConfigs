import importlib
import json
import os

import coffea
import law
import law.decorator
import luigi
import luigi.util
from coffea.processor import Runner as CoffeaRunner
from coffea.util import load as load_coffea
from omegaconf import OmegaConf
from pocket_coffea.executors import executors_base
from pocket_coffea.parameters import defaults as parameters_utils
from pocket_coffea.utils import utils as pocket_utils
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.dataset import build_datasets
from pocket_coffea.utils.plot_utils import PlotManager


def extract_executor_and_site(executor: str) -> tuple:
    """Extract executor and site from executor string

    Parameters
    ----------
    executor : str
        name of the executor

    Returns
    -------
    tuple
        name of the executor and cluster
    """
    if "@" in executor:
        executor_name, site = executor.split("@")
    else:
        executor_name = executor
        site = None
    return executor_name, site


class baseconfig(luigi.Config):
    """config class that holds general parameters"""

    cfg = luigi.Parameter(
        description="Config file with parameters specific to the current run",
        default=os.path.join(os.getcwd(), "config.py"),
    )
    output_dir = luigi.Parameter(
        description="Output directory for the coffea processor and plots",
        default=os.path.join(os.getcwd(), "output"),
    )
    test = luigi.BoolParameter(
        description="Run with limit 1 interactively", default=False
    )


class CreateDatasets(law.Task):
    """Create Datasets Task: Create datasets for analysis"""

    dataset_definition = luigi.Parameter(
        description="json file containing the datasets definitions",
        default=os.path.join(os.getcwd(), "datasets", "datasets_definitions.json"),
    )
    keys = luigi.TupleParameter(
        description=(
            "Keys of the datasets to be created."
            "If None, the keys are read from the datasets definition file."
        ),
        default=(),
    )
    download = luigi.BoolParameter(
        description="Download datasets from DAS, default: False", default=False
    )
    overwrite = luigi.BoolParameter(
        description="Overwrite existing .json datasets, default: False", default=False
    )
    check = luigi.BoolParameter(
        description="Check existence of the datasets, default: False", default=False
    )
    split_by_year = luigi.BoolParameter(
        description="Split datasets by year, default: False", default=False
    )
    local_prefix = luigi.Parameter(
        description="Prefix of the local path where the datasets are stored",
        default="",
    )
    allowlist_sites = luigi.TupleParameter(
        description="List of sites to be whitelisted", default=()
    )
    blocklist_sites = luigi.TupleParameter(
        description="List of sites to be blacklisted", default=()
    )
    regex_sites = luigi.Parameter(
        description="Regex string to be used to filter the sites", default=""
    )
    parallelize = luigi.IntParameter(
        description=(
            "Number of parallel processes to be used to fetch the datasets, "
            "default: 4"
        ),
        default=4,
    )

    def read_datasets_definition(self):
        with open(self.dataset_definition) as f:
            return json.load(f)

    def create_datasets_paths(self):
        """Create set of paths for datasets output files"""
        datasets = self.read_datasets_definition()
        datasets_paths = set()
        for dataset in datasets.values():
            filepath = os.path.abspath(dataset["json_output"])

            if self.split_by_year:
                years = {
                    dataset_file["metadata"]["year"]
                    for dataset_file in dataset["files"]
                }

                # append year if split by year is true
                for year in years:
                    datasets_paths.update(
                        [
                            filepath.replace(".json", f"_{year}.json"),
                            filepath.replace(".json", f"_redirector_{year}.json"),
                        ]
                    )
            else:
                datasets_paths.update(
                    [filepath, filepath.replace(".json", "_redirector.json")]
                )
        return datasets_paths

    def output(self):
        """json files for datasets"""
        return [law.LocalFileTarget(d) for d in self.create_datasets_paths()]

    def run(self):
        build_datasets(
            self.dataset_definition,
            keys=self.keys,
            download=self.download,
            overwrite=self.overwrite,
            check=self.check,
            split_by_year=self.split_by_year,
            local_prefix=self.local_prefix,
            allowlist_sites=self.allowlist_sites,
            blocklist_sites=self.blocklist_sites,
            regex_sites=self.regex_sites,
            parallelize=self.parallelize,
        )


# @luigi.util.requires(CreateDatasets)
@luigi.util.inherits(baseconfig)
class Runner(law.Task):
    """Runner task: runs the coffea processor"""

    # parameters
    coffea_output = luigi.Parameter(
        description="Filename of the coffea output", default="output.coffea"
    )
    limit_files = luigi.IntParameter(description="Limit number of files", default=None)
    limit_chunks = luigi.IntParameter(
        description="Limit number of chunks", default=None
    )
    executor = luigi.Parameter(
        description=(
            "Overwrite executor from config (to be used only with the --test options)"
        ),
        default="iterative",
    )
    scaleout = luigi.IntParameter(description="Overwrite scalout config", default=None)
    process_separately = luigi.BoolParameter(
        description="Process each dataset separately", default=False
    )

    # init class attributes
    run_options = {}
    config = None
    processor_instance = None
    site = None

    def requires(self):
        return CreateDatasets.req(self)

    def load_config(self):
        config_module = pocket_utils.path_import(self.cfg)

        try:
            config = config_module.cfg
            config.save_config(self.output_dir)
        except AttributeError as e:
            raise AttributeError(
                "The provided configuration module does not contain a"
                "`cfg` attribute of type Configurator. Please check your configuration!"
            ) from e

        if not isinstance(config, Configurator):
            raise TypeError(
                "`cfg`in config file is not of type Configurator."
                "Please check your configuration!"
            )

        self.config = config
        self.processor_instance = config.processor_instance
        if hasattr(config_module, "run_options"):
            self.run_options = config_module.run_options

    def load_run_options(self):
        self.executor, self.site = extract_executor_and_site(self.executor)

        run_options_defaults = parameters_utils.get_default_run_options()
        run_options_general = run_options_defaults["general"]

        if self.site in run_options_defaults:
            run_options_general.update(run_options_defaults[self.site])

        if self.executor in run_options_defaults:
            run_options_general.update(run_options_defaults[self.executor])

        if f"{self.executor}@{self.site}" in run_options_defaults:
            run_options_general.update(
                run_options_defaults[f"{self.executor}@{self.site}"]
            )

        # merge user defined run_options
        # if self.run_options is not None:
        self.run_options = parameters_utils.merge_parameters(
            run_options_general, self.run_options
        )

        if self.test:
            self.run_options["executor"] = self.executor
            # will be overwritten if user specifies it
            self.run_options["limit-files"] = (
                self.limit_files if (self.limit_files is not None) else 2
            )
            self.run_options["limit-chunks"] = (
                self.limit_chunks if (self.limit_chunks is not None) else 2
            )
            self.config.filter_dataset(self.run_options["limit-files"])

        if self.scaleout is not None:
            self.run_options["scaleout"] = self.scaleout

    def get_executor(self):
        """load the executor factory and return the executor instance"""
        # load executor
        if self.site is not None:
            executor_module = importlib.import_module(
                f"pocket_coffea.executors.executors_{self.site}"
            )
        else:
            executor_module = importlib.import_module(
                "pocket_coffea.executors.executors_base"
            )

        # load the executor factory
        executor_factory = executor_module.get_executor_factory(
            self.executor, run_options=self.run_options, outputdir=self.output_dir
        )
        if not isinstance(executor_factory, executors_base.ExecutorFactoryABC):
            raise TypeError(
                f"Executor factory is not of type {executors_base.ExecutorFactoryABC}"
            )

        return executor_factory.get()

    def process_datasets(self, executor):
        """Process the dataset with the given executor and the run_options"""
        if self.process_separately:
            raise NotImplementedError(
                "separate processing for each dataset is not implemented yet"
            )
        else:
            fileset = self.config.filesets

            run = CoffeaRunner(
                executor=executor,
                chunksize=self.run_options["chunksize"],
                maxchunks=self.run_options["limit-chunks"],
                skipbadfiles=self.run_options["skip-bad-files"],
                schema=coffea.processor.NanoAODSchema,
                format="root",
            )
            output = run(
                fileset, treename="Events", processor_instance=self.processor_instance
            )
            # save(output)
            coffea.util.save(output, self.output()["coffea_output"].path)

    @property
    def skip_output_removal(self):
        return not self.test

    def output(self):
        return {
            key: law.LocalFileTarget(
                os.path.join(os.path.abspath(self.output_dir), filename)
            )
            for key, filename in [
                ("coffea_output", self.coffea_output),
                ("parameters", "parameters_dump.yaml"),
            ]
        }

    @law.decorator.safe_output
    def run(self):
        # create output folder if it does not exist
        self.output()["coffea_output"].parent.touch()

        # load configurator
        self.load_config()
        self.load_run_options()

        executor = self.get_executor()
        self.process_datasets(executor)


# @luigi.util.requires(Runner)
# NOTE: https://luigi.readthedocs.io/en/stable/api/luigi.util.html#using-inherits-and-requires-to-ease-parameter-pain
# requires decorator also inherits all parameters, not sure if thats what we should do
@luigi.util.inherits(baseconfig)
class Plotter(law.Task):
    plot_dir = luigi.Parameter(default="plots", description="Output folder for plots")
    plot_verbose = luigi.IntParameter(
        default=0, description="verbosity level for PlotManager (default: 0)"
    )
    plotting_style = luigi.Parameter(
        default="", description="yaml file with plotting style"
    )
    blind = luigi.BoolParameter(
        default=True, description="If True, only MC is plotted. default=True"
    )

    def requires(self):
        return Runner.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self.output_dir, self.plot_dir, ".plots_done")
        )

    def load_plotting_style(self):
        parameters = OmegaConf.load(self.input()["parameters"].abspath)
        if os.path.isfile(self.plotting_style):
            parameters = parameters_utils.merge_parameters_from_files(
                parameters, self.plotting_style, update=True
            )
        elif self.plotting_style != "":
            print(f"file {self.plotting_style} not found. Using default style")

        OmegaConf.resolve(parameters)
        style = parameters["plotting_style"]
        samples = self.load_datasample_names()

        if self.blind:
            exclude_samples = [sample for sample in samples if "DATA" in sample]
            OmegaConf.update(style, "exclude_samples", exclude_samples)
            print(f"excluding samples from plotting: {exclude_samples}")
        return style

    def load_datasample_names(self):
        with open(os.path.join(self.output_dir, "config.json")) as f:
            config = json.load(f)
        return config["datasets"]["samples"]

    def run(self):
        inp = self.input()

        output_coffea = load_coffea(inp["coffea_output"].abspath)
        parameters = self.load_plotting_style()

        plotter = PlotManager(
            variables=output_coffea["variables"].keys(),
            hist_objs=output_coffea["variables"],
            datasets_metadata=output_coffea["datasets_metadata"],
            plot_dir=os.path.join(self.output_dir, self.plot_dir),
            style_cfg=parameters,
            verbose=self.plot_verbose,
        )

        plotter.plot_datamc_all(syst=False)
