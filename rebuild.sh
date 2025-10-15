#!/bin/bash
make clean
make
sudo make install
sudo systemctl restart postgresql
