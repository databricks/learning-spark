#!/usr/bin/perl
use strict;
use warnings;

# This is a simple perl example of how to work with pipe interface
# Here we read in each line and ouput the corresponding words
# This is equivelent to rdd.flatMap(_.split($SEPARATOR))
while (my $line = <>) {
    chomp ($line);
    my @words = split($ENV{'SEPARATOR'}, $line);
    foreach my $word (@words) {
	print $word."\n";
    }
}
