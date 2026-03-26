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

    """Test if json_to_stream converts run-params block to a stream"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_json_to_stream_run_params(self, load_json_file):
        run_params_stream = blockbreaker.json_to_stream(load_json_file, "run-params", 0)
        expected_stream = self._load_file("output-oslat-run-params.stream")

        assert run_params_stream == expected_stream

    """Test if dump_json returns a str"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_dump_json_mvparams(self, load_json_file):
        assert type(load_json_file) == dict
        benchmark_blk = blockbreaker.get_mv_params(load_json_file, "oslat")
        input_json = blockbreaker.dump_json(benchmark_blk, "mv-params", 0)
        assert type(input_json) == str

    """Test if json_to_stream returns None for invalid index"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_json_to_stream_endpoints_invalid_idx(self, load_json_file, capsys):
        input_json = blockbreaker.json_to_stream(load_json_file, "endpoints", 1)
        out, err = capsys.readouterr()
        assert out == ""
        assert input_json is None

    """Test validate_schema using default schema for null schema_file arg"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_validate_schema_default(self, load_json_file):
        validated_json = blockbreaker.validate_schema(load_json_file)
        assert validated_json is True

    """Test validate_schema using osp schema and returns True"""
    @pytest.mark.parametrize("load_json_file",
                             [ "input-oslat-osp.json" ], indirect=True)
    def test_validate_schema_endpoint_osp(self, load_json_file):
        validated_json = blockbreaker.validate_schema(
                             load_json_file["endpoints"][0], "schema-osp.json")
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

