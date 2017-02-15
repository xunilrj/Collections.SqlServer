Set-Location (Split-Path $PSCommandPath -Parent)
.\build.ps1
"CLEAR`nADD:KEY1:VALUE1`nREMOVE:Key1`nSET:Key1:Value1`nSET:Key2:Value2`nSET:Key2:NewValue2`nEND" > commands.txt
start ".\sources\SqlDictionaryTest\bin\Debug\SqlDictionaryTest.exe" '-c "data source=.;initial catalog=KeyValueDB;user id=sa;password=12345678a" -t KeyValueTable -k Key -v Value' -RedirectStandardInput .\commands.txt -RedirectStandardOutput out.txt -Wait
gc out.txt