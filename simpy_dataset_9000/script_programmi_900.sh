echo Creatori eventi:
cd creazione_tel_tipizz
{
python3 -u creatore_eventi_1.py
python3 -u creatore_eventi_2.py
python3 -u creatore_eventi_3.py
python3 -u creatore_eventi_4.py
} &
wait

echo Miscellanous Creatore eventi
python3 -u creazione_tel_tipizz/miscellanous.py &
wait

cd ..
echo Significativita eventi:
{
python3 -u events1_sign.py
python3 -u events2_sign.py
python3 -u events3_sign.py
python3 -u events4_sign.py
} &
wait

echo Vettorizzazione:
python3 -u miscellanous.py &>> outputmisc.log &

