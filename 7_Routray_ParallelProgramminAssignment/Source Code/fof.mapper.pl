#!/usr/bin/perl

$, = " ";
$\ = "\n";

while (<>) 
{
  @friends = split( /\s+/ );
  $size = @friends;

  $mf = $friends[0];
  for($i = 1; $i<$size; $i++)
  {
	  $kf = $friends[$i];
	  for($j = 1; $j < $size; $j++)
	  {
		  if($j!=$i)
		  {
			  if($mf<$friends[$j])
			  {
				  print $kf, $mf, $friends[$j];
			  }
			  else
			  {
				  print $kf, $friends[$j], $mf;
			  }
		  }
	  }
  }
}
