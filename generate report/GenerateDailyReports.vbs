Option Explicit

Dim excel
Dim clearingBroker(5)
Dim broker

Set excel = CreateObject("Excel.Application")

clearingBroker(0) = "EDF_Man"
clearingBroker(1) = "Mizuho"
clearingBroker(2) = "HplpNewedge"
clearingBroker(3) = "HppgNewedge"
clearingBroker(4) = "HplpWells_Fargo"
clearingBroker(5) = "HppgWells_Fargo"

For Each broker In clearingBroker
	excel.Run "'\\Gateway\hetco\P003\Tasks\PCSPAN\Span4\Automation\DailyReports.xlsm'!GenerateDailyReports" & broker
Next

excel.Quit
Set excel = Nothing