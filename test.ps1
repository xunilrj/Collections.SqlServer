Set-Location (Split-Path $PSCommandPath -Parent)
.\build.ps1
start ".\sources\SqlDictionaryTest\bin\Debug\SqlDictionaryTest.exe" '-c "data source=.;initial catalog=KeyValueDB;user id=sa;password=12345678a" -t KeyValueTable -k Key -v Value' -RedirectStandardOutput out.txt -Wait
gc out.txt