### Schema

The files located here are used to validate schema for JSON files read or written by the rickshaw project.

#### Run-file and endpoint schemas (user-facing)

- `run-file.json`: Validates the user-supplied run-file (benchmarks, endpoints, tags, tool-params, run-params)
- `kube.json`: Validates a Kubernetes endpoint block
- `remotehosts.json`: Validates a remote hosts endpoint block
- `kvm.json`: Validates a KVM endpoint block
- `osp.json`: Validates an OpenStack Platform endpoint block

#### Internal data schemas

- `rickshaw-run.json`: Validates the internal rickshaw-run result data
- `benchmark.json`: Validates the benchmark config (how to run it, how to post-process the data)
- `tool.json`: Validates the tool config (how to run it, how to post-process the data)
- `utility.json`: Validates the utility config
- `bench-metric.json`: Validates the format to log metrics
- `sample-persistent-ids.json`: Validates persistent ID mappings for samples/periods
- `rickshaw-settings.json`: Validates the rickshaw settings configuration file
- `source-images-input.json`: Validates input for image sourcing operations
- `source-images-output.json`: Validates output from image sourcing operations
- `crucible-ci.json`: Validates CI job configuration

#### Schemas from other projects

- `bench-params.json`: Benchmark parameters schema
- `tool-params.json`: Tool parameters schema
