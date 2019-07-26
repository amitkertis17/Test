#!/bin/bash
echo "<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>candidate</title>
</head>
<body>
Hello visitor !!!  $1
</body>
</html>" > /usr/share/nginx/html/temp.html

COPY temp.html /usr/share/nginx/html/index1.html