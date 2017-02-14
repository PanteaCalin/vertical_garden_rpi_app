gcc build command line:
gcc -o vertical_garden_rpi_app vertical_garden_rpi_app.c bcm2835.c `mysql_config --cflags --libs`
