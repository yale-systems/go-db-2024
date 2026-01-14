#!/usr/bin/perl -w

use FileHandle;
use File::Basename;

my $label = $ARGV[0];
my $manifest_file = $ARGV[1];
my $outdir_prefix = "$ARGV[2]";
unless($label and $manifest_file) {
  print <<END;
Usage:

  strip.pl LABEL MANIFEST-FILE

Strip.pl is a very simple stream editor to remove/replace code.

Strip.pl finds all .java files from the current directory, and copies them to
a new directory (prepended by 6.830-LABEL, where LABEL is the command-line
parameter). Output will be a tarball called ${outdir_prefix}LABEL.

MANIFEST-FILE is a file containing the list of files to package in the tarball.

.java files may contain tags that indicate what to do: [in all these examples,
TAGS are simple keywords, possibly separated by '|'.

<strip TAGS>
</strip TAGS>

  Everything between these is replaced by "// TODO: some code goes here" if one of
  the keywords in TAGS matches LABEL.

<silentstrip TAGS>
</silentstrip TAGS>

  Everything between these is removed if one of the keywords in TAGS matches
  LABEL.

<insert TAGS>
</insert TAGS>

  Everything between these is inserted if one of the keywords in TAGS matches
  LABEL.
END
  exit;
}

# Necessary for lab1 (+ dependencies)
# src/simpledb/storage/TupleDesc.java
# src/simpledb/storage/Tuple.java
# test/simpledb/unittest/TupleTest
# test/simpledb/unittest/TupleDescTest
# src/simpledb/common/Catalog.java
# test/simpledb/unittest/CatalogTest
# src/simpledb/storage/HeapPageId.java
# src/simpledb/storage/RecordId.java
# src/simpledb/storage/HeapPage.java
# test/simpledb/unittest/HeapPageIdTest
# test/simpledb/unittest/RecordIdTest
# test/simpledb/unittest/HeapPageTest
# src/simpledb/storage/HeapFile.java
# test/simpledb/unittest/HeapFileTest
# src/simpledb/storage/BufferPool.java
# src/simpledb/execution/SeqScan.java
# test/simpledb/systemtest/ScanTest

my $outdir = "$outdir_prefix$label";

@files = ();
open(MANIFEST, "$manifest_file") or die("Cannot open $manifest_file");
while (my $line = <MANIFEST>) {
    print $line;
    push @files, $line;
}

#my @files = split /\n/, `find src -name *.java`;
system "rm -rf $outdir";

sub validlabel {
  my ($stuff) = @_;
  $stuff or return 0;
  my %labels = map { $_ => $_ } split '\|', $stuff;
  return exists $labels{$label};
}

foreach my $infile (@files) {
  my $outfile = "$outdir/$infile";
  my $dirname = dirname($outfile);
  -d $dirname or system "mkdir -p $dirname";

  my $ifh = new FileHandle("<$infile") or die "couldn't open $infile: $!\n";
  my $ofh = new FileHandle(">$outfile") or die "couldn't open $outfile: $!\n";

  my $stripping = 0;
  my $inserting = 0;
  my $lastempty = 0;
  while(<$ifh>) {
    if(m/^\s*$/) {
      $lastempty++ >= 1 and next;
    }

    if(m/(\s*)\/\/\s*<strip\s*(.*)>/) {
      &validlabel($2) or next;
      print $ofh "$1// TODO: some code goes here\n";
      $stripping = 1;
      next;

    } elsif(m/\s*<silentstrip\s*(.*)>/) {
      &validlabel($1) or next;
      $stripping = 1;
      next;

    } elsif(m/<\/strip>|\s*<\/silentstrip>/) {
      $stripping = 0;
      next;

    } elsif(m/<insert\s*(.*)>/) {
      if(&validlabel($1)) {
        $inserting = 1;
      } else {
        $stripping = 1;
      }
      next;

    } elsif(m/\s*<\/insert>/) {
      $inserting = $stripping = 0;
      next;
    }

    $stripping and next;
    $inserting and s/(\s*)\/\/\s*(.*)/$1$2/;

    $lastempty = 0 unless m/^\s*$/;
    print $ofh $_;
  }
  $ifh->close();
  $ofh->close();
}

system <<EOF;
EOF
