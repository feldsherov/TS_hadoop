#/bin/bash
cp test.txt test1.txt

for i in {1..30}
do 
    cat test1.txt | python streaming/mapper.py | sort | python streaming/reducer.py > test_tmp.txt
    mv test_tmp.txt test1.txt
done

rm -rf test_tmp.txt
mv test1.txt test_ans.txt