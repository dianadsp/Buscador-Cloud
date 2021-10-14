<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="description" content="Trabajo de Hadoop - Cloud Computing">
    <meta name="author" content="Diana Patino Polar">
    <meta name="generator" content="Jekyll v4.1.1">
    <title>Trabajo de Cloud Computing</title>

    <link rel="canonical" href="https://getbootstrap.com/docs/4.5/examples/starter-template/">

    <!-- Bootstrap core CSS -->
	<link href="bootstrap.min.css" rel="stylesheet">

    <style>
      .bd-placeholder-img {
        font-size: 1.125rem;
        text-anchor: middle;
        -webkit-user-select: none;
        -moz-user-select: none;
        -ms-user-select: none;
        user-select: none;
      }

      @media (min-width: 768px) {
        .bd-placeholder-img-lg {
          font-size: 3.5rem;
        }
      }


/* Center the loader */
#loader {
  position: absolute;
  left: 50%;
  top: 50%;
  z-index: 1;
  width: 150px;
  height: 150px;
  margin: -75px 0 0 -75px;
  border: 16px solid #f3f3f3;
  border-radius: 50%;
  border-top: 16px solid #3498db;
  width: 120px;
  height: 120px;
  -webkit-animation: spin 2s linear infinite;
  animation: spin 2s linear infinite;
}

@-webkit-keyframes spin {
  0% { -webkit-transform: rotate(0deg); }
  100% { -webkit-transform: rotate(360deg); }
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* Add animation to "page content" */
.animate-bottom {
  position: relative;
  -webkit-animation-name: animatebottom;
  -webkit-animation-duration: 1s;
  animation-name: animatebottom;
  animation-duration: 1s
}

@-webkit-keyframes animatebottom {
  from { bottom:-100px; opacity:0 } 
  to { bottom:0px; opacity:1 }
}

@keyframes animatebottom { 
  from{ bottom:-100px; opacity:0 } 
  to{ bottom:0; opacity:1 }
}

#myDiv {
  display: none;
  text-align: center;
}



    </style>
    <!-- Custom styles for this template -->
    <link href="starter-template.css" rel="stylesheet">
  </head>
  <body>
    <nav class="navbar navbar-expand-md navbar-dark bg-dark fixed-top">
  <a class="navbar-brand" href='?num=10&text=""'>Cloud Computing - Trabajo Hadoop</a>
  <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarsExampleDefault" aria-controls="navbarsExampleDefault" aria-expanded="false" aria-label="Toggle navigation">
    <span class="navbar-toggler-icon"></span>
  </button>

  <div class="collapse navbar-collapse" id="navbarsExampleDefault">
    <ul class="navbar-nav mr-auto">


        <a class="nav-link dropdown-toggle active" href="#" id="dropdown01" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">Diana Patiño</a>
        
	<li class="nav-item dropdown">

	-<div class="dropdown-menu" aria-labelledby="dropdown01">
          <a class="dropdown-item" href="#">Diana Patino Polar</a>
        </div>
		
	  </li>

    </ul>
    <form class="form-inline my-2 my-lg-0" action="index.php" method="get">
	
	  <select class="custom-select my-1 mr-sm-2" id="inlineFormCustomSelectPref" name="num">
		<option value="10">10</option>
		<option value="20">20</option>
		<option value="50">50</option>
		<option select value="100">100</option>
		<option value="250">250</option>
		<option value="500">500</option>
		<option value="900">900</option>
	  </select>
	
      <input class="form-control mr-sm-2" type="text" placeholder="Escribe Busqueda" name="text" size="45" aria-label="Search">
	  
      <button class="btn btn-secondary my-2 my-sm-0" type="submit">Buscar</button>
    </form>
  </div>
</nav>


<script>
var myVar;
function myFunction() {
  myVar = setTimeout(showPage, 3000);
}
function showPage() {
  document.getElementById("loader").style.display = "none";
  document.getElementById("myDiv").style.display = "block";
}
document.getElementById("demo").innerHTML = showPage();
</script>


<main role="main" class="container">




<?php

function generateRandomString($length = 10) {
    $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    $charactersLength = strlen($characters);
    $randomString = '';
    for ($i = 0; $i < $length; $i++) {
        $randomString .= $characters[rand(0, $charactersLength - 1)];
    }
    return $randomString;
}


if (isset ($_GET["text"]) or $_GET["text"]!=""){

	$file_name = generateRandomString();

	//echo "/home/diana_dsptesting/search/in/".$file_name.".txt";
	$myfile = fopen("/home/diana_dsptesting/search/in/".$file_name.".txt", "w") or die("Unable to open file!");
	
	$txt = htmlspecialchars($_GET["num"])." ".htmlspecialchars($_GET["text"]);
	fwrite($myfile, $txt);
	fclose($myfile);

	$output = "/home/diana_dsptesting/search/out/".$file_name.".txt";
	while (!file_exists($output)){
		usleep(10000);
	}
	//echo $output;
	$myfile = fopen($output, "r") or die("Unable to open file!");
	while(!feof($myfile)) {
		echo fgets($myfile);
	}
	fclose($myfile);
	unlink($output);
        //echo '<div class="alert alert-primary" role="alert">File Creado</div>';
	
}else{
        echo '<div class="alert alert-primary" role="alert">Comienza una Busqueda</div>';
}
?>

</div>



</main><!-- /.container -->

<script src="https://code.jquery.com/jquery-3.5.1.slim.min.js" integrity="sha384-DfXdz2htPH0lsSSs5nCTpuj/zy4C+OGpamoFVy38MVBnE+IbbVYUew+OrCXaRkfj" crossorigin="anonymous"></script>
      <script>window.jQuery || document.write('<script src="jquery.slim.min.js"><\/script>')</script><script src="bootstrap.bundle.min.js"></script>

</html>

