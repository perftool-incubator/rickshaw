# rickshaw
Rickshaw will run a benchmark for you.  It "takes" your benchmark wherever you need it to go, as long as there is an implementation for your particular endpoint (a host, cloud, container-runtime, etc).  These endpoints are in [./endpoints](https://github.com/perftool-incubator/rickshaw/tree/master/endpoints). As of this version endpoints exist for k8s, osp, and remotehost.

### Input

Rickshaw needs the following:
- Benchmark \[helper\] directory (--bench-dir):
  - To automate a benchmark execution, rickshaw needs a collection of configuration and scripts to assist running that benchmark.  This collection of config and scripts is usually not the benchmark itself; it is another project/git-repo which contains
    - config JSON which describes to rickshaw how to run the benchmark
    - any pre and post-processing scripts which aid in the execution of the benchmark.
    The "--bench-dir" argument should be used to provide the location of that benchmark [helper] project. 
- Benchmark parameters JSON file (--bench-params):
  - This tells rickshaw all the different ways you want to run the benchmark.  The [multiplex](https://github.com/perftool-incubator/multiplex) project can be used to generate this array (it can convert things like "--rw=read,write --bs=4k" into the proper JSON), and it will do parameter validation for you as well.
- Endpoints (--endpoint:\<endpoint-type\>:\<endpoint-options\>):
  - An endpoint is a place a benchmark or tool runs.  An endpoint could be almost anything as long as there is an implementation to support that endpoint type.  The most basic endpoint is 'local'.  Other endpoints planned are 'ssh' for executing on a remote host, 'k8s' for executing on kubernetes (with dynamic creation of pods/containers), 'osp' for execution on Openstack (with built-in support to create VMs on demand).  Other endpoints could exist, like 'ec2' for Amazon cloud, 'gce' for Google cloud, and 'azure' for Microsoft cloud.
  - Specifying the endpoint (and what clients/servers it will run) determines how the benchmark gets executed on different systems.  The default endpoint, local, simply runs the benchmark command on the local host.  Rickshaw supports using multiple endpoints for the same run.  For example, if you want to run uperf benchmark, you need both a client and server uperf.  If you want to run the uperf server on Kuberbetes, but you want to run the uperf client on a baremetal host, you can use the 'k8s' endpoint for the server and the 'ssh' endpoint for the client.
  - <pre>--endpoint:k8s:server[1]:$master-hostname --endpoint:ssh:client[1]:$client-hostname</pre>
  - Other examples
    - 8 servers running in 8 containers in k8s and 8 clients running on the same barmetal host:  
      <pre>--endpoint:k8s:server[1-8]:$master-hostname --endpoint:ssh:client[1-8]:$client-hostname</pre>
    - 8 servers running in 8 containers in k8s and 8 clients running on 8 different barmetal host:  
      <pre>--endpoint:k8s:server[1-8]:$master-hostname --endpoint:ssh:client[1]:$client1-hostname \
      --endpoint:ssh:client[2]:$client2-hostname --endpoint:ssh:client[3]:$client3-hostname \
      --endpoint:ssh:client[4]:$client4-hostname --endpoint:ssh:client[5]:$client5-hostname \
      --endpoint:ssh:client[6]:$client6-hostname --endpoint:ssh:client[7]:$client7-hostname \
      --endpoint:ssh:client[8]:$client8-hostname </pre>  
    - 8 servers running in 8 containers in k8s cluster A and 8 clients running in 8 containers in k8s cluster B:  
      <pre>--endpoint:k8s:server[1-8]:$cluster-a-master-hostname --endpoint:k8s:client[1-8]:$cluster-b-master-hostname</pre>
  - If you don't provide any endpoint options, rickshaw will assume you want to run on the local system, and only 1 client will be used, and if the benchmark requires a server, only 1 server will be used, also on the local host.  
- User and test information
  - The following is optional, but is highly recommended to use so that your run can be easily searchable later.  Rickshaw looks for the following
    - RS_NAME Environment variable or --name option with user's full name "First Last"
    - RS_EMAIL Environment variable or --email option with user's email address "my-email@email.domain"
    - RS_TAGS Environment variable or --tags containing a comma-separated list of words that are relevant to the run
    - RS_DESC Environment variable or --desc containing a free form description of the purpose, conditions, or any other relevant information about this test.

Other, optional paramers include:
- Specifying the test order (--test-order):
  - 'i-s' = run one iteration and all its samples, then run the next iteration and its samples, etc.
  - 's-i' = run the first sample for all iterations, then run all the iterations for the second sample, etc.
- Specifying the number of samples per iteration (--num-samples):
  - \<int\> = number of sample executions to run per iteration

Example rickshaw command:

rickshaw --num-samples=2 --tags test,beta,fail --email name@my.domain --name "John Doe"  --bench-config=testing/fio-bench-config.json --bench-params=testing/fio-user-params.json --endpoint=local:client[1] --test-order=s-i
    
  
### Output
  
Rickshaw will provide the following:
- Human readable log of the rickshaw execution, including any warning and errors from other projects, tools, or benchmarks it used.
- A JSON file or Elastic documents describing the benchmark run that was handled by rickshaw
- Raw output from tools and benchmarks used.
- Post-processed data (optionally) from tools and benchmarks
  - Tools and benchmarks run by rickshaw should have post-processing programs to convert its native output to [CommonDataModel](https://github.com/perftool-incubator/CommonDataModel)
  - Post-processing this data can be done (and re-done) at a later time if the user chooses.
  
