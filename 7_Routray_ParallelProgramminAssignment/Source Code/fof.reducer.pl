#!/usr/bin/perl
$, = " ";
$\ = "\n";

$l = -1;
$lf1 = -1;
$lf2 = -1;


while (<>)
{
  @triplet = split;

  if ( ($triplet[0] == $l) && ($triplet[1] == $lf1) && ($triplet[2] == $lf2 ) )
  {
  print $triplet[0], '<',$triplet[0],',',$triplet[1],',',$triplet[2],'>';
  }
  else
  {
    $l = $triplet[0];
    $lf1 = $triplet[1];
    $lf2 = $triplet[2];
  }
}
