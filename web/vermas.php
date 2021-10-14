<?php


function highlight($text, $words) {
    preg_match_all('~\w+~', $words, $m);
    if(!$m)
        return $text;
    $re = '~\\b(' . implode('|', $m[0]) . ')\\b~i';
    return preg_replace($re, '<b>$0</b>', $text);
}

function highlightWords($string, $term){
    $term = preg_replace('/\s+/', ' ', trim($term));
    $words = explode(' ', $term);

    $highlighted = array();
    foreach ( $words as $word ){
        $highlighted[] = "<span class='highlight'>".$word."</span>";
    }

    return str_replace($words, $highlighted, $string);
}

if (isset ($_GET["file"]) or $_GET["words"]!=""){

	$output = htmlspecialchars($_GET["file"]);
	$words = explode(",", htmlspecialchars($_GET["words"]));
	//print_r($words);

	//echo "/home/diana_dsptesting/database/out/".$output.".txt";
	$myfile = fopen("/home/diana_dsptesting/database/out/".$output.".txt", "r") or die("Unable to open file!");
	$str_data = "";

        while(!feof($myfile)) {
                $str_data = $str_data.fgets($myfile)."<br>";
        }
	
	echo $str_data;

}

?>
