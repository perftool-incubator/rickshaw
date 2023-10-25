#!/usr/bin/env python3

import pytest
import glob
import validate_run_file
import sys

class TestValidateRunFile:

    """Validate all ci runfiles"""
    @pytest.mark.parametrize("runfile", glob.glob("tests/JSON/ci-*.json"))
    def test_validate_run_file(self, runfile, capsys):
        validate = validate_run_file.validate(runfile)
        out, err = capsys.readouterr()
        assert validate == 0
        assert "[ OK ]" in out
        assert err == ""
