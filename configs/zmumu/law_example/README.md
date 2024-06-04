# Full analysis example using law workflow

This section only covers the basic usage of law with the example of a Z->mumu analysis. For a more detailed description of the analysis, please refer to the [Z->mumu Analysis](https://pocketcoffea.readthedocs.io/en/latest/analysis_example.html) documentation.

## Quick Introduction into law

law is a workflow management tool that is used to run the analysis. For a detailed description of law, please refer to the [law documentation](https://law.readthedocs.io/en/latest/).

Different parts of the analysis are split into tasks (`law.Task`). These tasks can `require` other tasks and produce an `output`. `Parameters` are used to customize the tasks and can control the execution behaviour. A basic task looks like this:
    
```python
import law
import luigi

class Runner(law.Task):
    cfg = luigi.Parameter()

    def output(self):
        return law.LocalFileTarget("output.coffea")

    def requires(self):
        return CreateDatasets.req(self)

    def run(self):
        # load the output of the CreateDatasets task
        datasets = self.input()

        # do something...
        process(datasets, self.cfg)
```

The task `Runner` is executed by the command `law run Runner --cfg my_config.py`. law will automatically resolve the dependencies and execute the tasks in the correct order. Before execution it checks wether the output is already present and skips the task if it is. The status of the tasks can be checked with `law run Runner --cfg my_config.py --print-status -1`. This prints the status of the task and all its dependencies (`-1` specifies the depth which in this case means all dependencies, `0` will print only the tasks itself, `1` the task and its first dependence).

## Running the Z->mumu analysis

### Setup

The analysis is split into three tasks
- `CreateDatasets`: creates the dataset files from the datasets definition
- `Runner`: processes the datasets according to the configuration
- `Plotter`: creates the plots

The tasks are defined in the `law_tasks.py` file.

To run the analysis you need to setup the environment, follow the instructions in the [installation](https://pocketcoffea.readthedocs.io/en/latest/installation.html) documentation. After the installation you have to source the `setup.sh` file in the `law_example` folder, which sets up specific environment variables for the analysis.
```bash
source setup.sh
```

### Execute the Tasks
Once everything is setup successfully you can check the status of the analysis with the following command:
```bash
law run Plotter --cfg config.py --print-status -1
```
If you want to run the analysis for testing you can use the following command:
```bash
law run Plotter --cfg config.py --test
```
To check which parameters are available for a given task you can use the `--help` flag:
```bash
law run Plotter --help
```
Parameters of subsequent tasks can be set too by explicitly specifying the task:
```bash
law run Plotter --cfg config.py --Runner-executor dask@lxplus --CreateDatasets-regex-sites 'T[123]_(FR|IT|DE|BE|CH|UK)_\w+'
```
This will set the executor to `dask@lxplus` and the sites used for the dataset creation are limited to the specified regex.

To delete the output of a task you can use the `--remove-output <DEPTH>` flag which will delete the output of the task and all its dependencies up to the specified depth. For example to delete the output of the `Plotter` task and all its dependencies you can use:
```bash
law run Plotter --cfg config.py --remove-output -1
```
This flag also takes a comma separated list of arguments `--remove-output <DEPTH>,<MODE>,<RUN>`:
- `DEPTH`: the depth of the dependencies that should be removed
- `MODE`: `a(ll)`, `i(nteractive)`, `d(ry)`
- `RUN`: 0/1 (False/True) wether to run the tasks after the deletion