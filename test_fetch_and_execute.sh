# !bin/bash

echo "wget -nd -v -i datasets.txt\n"
wget -nd -v -i datasets.txt;
echo "\npython3.7 load_data.py\n";
python3.7 load_data.py;
rm *.csv;