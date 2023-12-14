#!/usr/bin/env python3

import pytest
import json
import blockbreaker
import re

class TestBlockBreaker:

    # helper function to read stream from file
    def _load_file(self, filename):
        with open("tests/JSON/" + filename, "r") as file:
            data = file.read().rstrip("\n")
        return data

    # fixture to load json object from file
    @pytest.fixture(scope="function")
    def load_json_file(self, request):
        return blockbreaker.load_json_file("tests/JSON/" + request.param)

    """Test if json_to_stream converts endpoints block to a stream"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_json_to_stream_endpoints(self, load_json_file):
        endpoints_stream = blockbreaker.json_to_stream(load_json_file, "endpoints", 0)
        expected_stream = self._load_file("output-oslat-k8s-endpoints.stream")

        # endpoints config generates random stream, so we match only general args
        assert expected_stream in endpoints_stream
        assert 'securityContext:client-1:' in endpoints_stream
        assert 'resources:client-2:' in endpoints_stream
        assert 'annotations:server-1:' in endpoints_stream

    """Test if json_to_stream converts tags block to a stream"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_json_to_stream_tags(self, load_json_file):
        tags_stream = blockbreaker.json_to_stream(load_json_file, "tags", 0)
        expected_stream = self._load_file("output-oslat-tags.stream")

        assert tags_stream == expected_stream

    """Test if json_to_stream converts run-params block to a stream"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_json_to_stream_run_params(self, load_json_file):
        run_params_stream = blockbreaker.json_to_stream(load_json_file, "run-params", 0)
        expected_stream = self._load_file("output-oslat-run-params.stream")

        assert run_params_stream == expected_stream

    """Test if dump_json returns a str"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_dump_json_mvparams(self, load_json_file):
        assert type(load_json_file) == dict
        benchmark_blk = blockbreaker.get_mv_params(load_json_file, "oslat") 
        input_json = blockbreaker.dump_json(benchmark_blk, "mv-params", 0)
        assert type(input_json) == str

    """Test if json_to_stream returns None for invalid index"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_json_to_stream_endpoints_invalid_idx(self, load_json_file, capsys):
        input_json = blockbreaker.json_to_stream(load_json_file, "endpoints", 1)
        out, err = capsys.readouterr()
        assert out == ""
        assert input_json is None

    """Test validate_schema using default schema for null schema_file arg"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_validate_schema_default(self, load_json_file):
        validated_json = blockbreaker.validate_schema(load_json_file)
        assert validated_json is True

    """Test validate_schema using k8s schema and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_validate_schema_endpoint_k8s(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-k8s.json")
        assert validated_json is True

    """Test validate_schema using osp schema and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_validate_schema_endpoint_osp(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-osp.json")
        assert validated_json is True

    """Test validate_schema using remotehost schema and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-remotehost.json" ], indirect=True)
    def test_validate_schema_endpoint_remotehost(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-remotehost.json")
        assert validated_json is True

    """Test validate_schema using kvm schema and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-kvm.json" ], indirect=True)
    def test_validate_schema_endpoint_kvm(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-kvm.json")
        assert validated_json is True

    """Test validate_schema using invalid schema and returns False"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-invalid.json" ], indirect=True)
    def test_validate_schema_endpoint_invalid(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-invalid.json")
        assert validated_json is False

    """Test validate_schema w/ missing endpoint type and returns False"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-notype.json" ], indirect=True)
    def test_validate_schema_endpoint_notype(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-null.json")
        assert validated_json is False

    """Test validate_schema using multiple endpoints and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s-osp.json" ], indirect=True)
    def test_validate_schema_endpoint_k8s_osp(self, load_json_file):
        validated_json_1 = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-k8s.json")
        validated_json_2 = blockbreaker.validate_schema(
                             load_json_file["endpoints"][1], "schema-osp.json")
        endpoints_stream = blockbreaker.json_to_stream(load_json_file, "endpoints", 1)
        expected_stream = self._load_file("output-oslat-k8s-osp.stream")

        assert validated_json_1 is True
        assert validated_json_2 is True
        # endpoint config generates random stream, so we match only general args
        assert expected_stream in endpoints_stream
        assert 'custom:client-1:' in endpoints_stream

    """Test validate_schema using multiple endpoints and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-multibench-k8s-remotehost.json" ], indirect=True)
    def test_validate_schema_multibench_k8s_remotehost(self, load_json_file):
        validated_json_0 = blockbreaker.validate_schema(
                             load_json_file, "schema.json")
        validated_json_1 = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-k8s.json")
        validated_json_2 = blockbreaker.validate_schema(
                             load_json_file["endpoints"][1], "schema-remotehost.json")
        endpoints_stream0 = blockbreaker.json_to_stream(load_json_file, "endpoints", 0)
        endpoints_stream1 = blockbreaker.json_to_stream(load_json_file, "endpoints", 1)
        expected_stream = self._load_file("output-multibench-k8s-remotehost.stream")

        assert validated_json_0 is True
        assert validated_json_1 is True
        assert validated_json_2 is True
        # endpoint config generates random stream, so we match only general args
        assert expected_stream in endpoints_stream0
        assert 'remotehost,host:ENDPOINT_HOST,user:ENDPOINT_USER' in endpoints_stream1
        assert 'server:0,client:1+5' in endpoints_stream1

    """Test if dump_json returns the correct json block"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-k8s.json" ], indirect=True)
    def test_dump_json_mvparams_object(self, load_json_file):
        assert type(load_json_file) == dict
        benchmark_blk = blockbreaker.get_mv_params(load_json_file, "oslat")
        input_json = blockbreaker.dump_json(benchmark_blk, "mv-params", 0)
        expected_output = self._load_file("output-oslat-k8s-mvparams.json")
        assert input_json == expected_output

    """Test if dump_json returns the correct json block from mv-params for uperf"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-multibench-k8s-remotehost.json" ], indirect=True)
    def test_dump_json_mvparams_index1(self, load_json_file):
        assert type(load_json_file) == dict
        benchmark_blk = blockbreaker.get_mv_params(load_json_file, "uperf")
        input_json = blockbreaker.dump_json(benchmark_blk, "mv-params", 0)
        expected_output = self._load_file("output-multibench-k8s-remotehost-mvparams1.json")
        assert input_json == expected_output

    """Test if get_bench_ids returns the correct stream"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-number-lists.json" ], indirect = True)
    def test_get_bench_ids_benchmarks(self, load_json_file):
        benchmarks_stream = blockbreaker.get_bench_ids(load_json_file, "benchmarks")
        expected_stream = self._load_file("output-number-lists-benchmarks.stream")

        assert expected_stream == benchmarks_stream

    """Test if json_to_stream properly processes input-number-lists.json endpoint 0"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-number-lists.json" ], indirect = True)
    def test_json_to_stream_endpoints_number_lists_0(self, load_json_file):
        endpoint_0_stream = blockbreaker.json_to_stream(load_json_file, "endpoints", 0)
        expected_stream = self._load_file("output-number-lists-endpoints-0.stream")

        assert expected_stream == endpoint_0_stream

    """Test if json_to_stream properly processes input-number-lists.json endpoint 1"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-number-lists.json" ], indirect = True)
    def test_json_to_stream_endpoints_number_lists_1(self, load_json_file):
        endpoint_1_stream = blockbreaker.json_to_stream(load_json_file, "endpoints", 1)
        expected_stream = self._load_file("output-number-lists-endpoints-1.stream")

        assert expected_stream == endpoint_1_stream
