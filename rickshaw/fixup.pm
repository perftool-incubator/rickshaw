# -*- mode: perl; indent-tabs-mode: nil; perl-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=perl

package rickshaw::fixup;

use JSON::XS;
use toolbox::json;
use toolbox::logging;

use Exporter qw (import);
our @EXPORT = qw(rickshaw_run_schema_fixup);

use strict;
use warnings;

sub rickshaw_run_schema_fixup {
    my $result_file = shift;
    chomp $result_file;
    if (! defined $result_file) {
	log_print "rickshaw_run_schema_fixup(): result_file not defined\n";
	return 1;
    }
    my $result_schema_file = shift;
    chomp $result_schema_file;
    if (! defined $result_schema_file) {
	log_print "rickshaw_run_schema_fixup(): result_schema_file not defined\n";
	return 1;
    }

    my $coder = JSON::XS->new->canonical->pretty;

    # this is a little uncoventianal because we are purposefully
    # bypassing toolbox's JSON functions in order to avoid JSON
    # validation.  we discovered some problems in the rickshaw-run
    # JSON schema that we have corrected, but if we are indexing data
    # produced before this correction then we need to manually update
    # the rickshaw-run to match the current schema without the input
    # data being run through JSON validation
    (my $file_rc, my $result_fh) = open_read_text_file($result_file);
    if ($file_rc == 0 and defined $result_fh) {
	log_print "Checking if the rickshaw-run data matches the current JSON schema\n";

	my $result_text = "";
	while ( <$result_fh> ) {
	    $result_text .= $_;
	}
	close($result_fh);
	chomp $result_text;

	my $result_ref = $coder->decode($result_text);
	if (not defined $result_ref) {
	    log_print "The rickshaw-run data is not valid JSON\n";
	    return 1;
	}

	my $validation_result = validate_schema($result_schema_file, $result_file, $result_ref);
	if ($validation_result != 0) {
	    log_print "Attempting to update the rickshaw-run data to match the current JSON schema\n";

	    # force these parameters to be integers instead of strings by
	    # "numifying" them
	    $$result_ref{'max-rb-attempts'} += 0;
	    $$result_ref{'max-sample-failures'} += 0;
	    $$result_ref{'num-samples'} += 0;

	    # try again
	    $validation_result = validate_schema($result_schema_file, $result_file, $result_ref);
	    if ($validation_result == 0) {
		my $update_rc = put_json_file($result_file, $result_ref, $result_schema_file);
		if ($update_rc != 0) {
		    log_print "Unable to update the rickshaw-run file after updating to match the current JSON schema\n";
		    return 1;
		} else {
		    log_print "Updating the rickshaw-run file to match the current JSON schema succeeded\n";
		}
	    } else {
		log_print "Unable to validate rickshaw-run data after updating to match the current JSON schema\n";
		return 1;
	    }
	}
    } else {
	log_print "Could not open the rickshaw-run for JSON Schema correction\n";
	return 1;
    }

    return 0;
}

1;
