python3 ./kraken.py --run load --pair XXRPZUSD --from_date 20180601 --to_date 20180602 --output_dir ../data/xrp
python3 ./kraken.py --run load --pair XETHZUSD --from_date 20180601 --to_date 20180602 --output_dir ../data/eth
python3 ./kraken.py --run load --pair XLTCZUSD --from_date 20180601 --to_date 20180602 --output_dir ../data/ltc
python3 ./kraken.py --run load --pair XXBTZUSD --from_date 20180601 --to_date 20180602 --output_dir ../data/xbt

python3 ./kraken.py --run csv --output_dir ../data/xrp --csv_file ../data/xrp.csv
python3 ./kraken.py --run csv --output_dir ../data/eth --csv_file ../data/eth.csv
python3 ./kraken.py --run csv --output_dir ../data/ltc --csv_file ../data/ltc.csv
python3 ./kraken.py --run csv --output_dir ../data/xbt --csv_file ../data/xbt.csv




