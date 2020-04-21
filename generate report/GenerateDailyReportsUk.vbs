Option Explicit

Dim excel
Dim accounts(1)
Dim account

Set excel = CreateObject("Excel.Application")

'accounts(0) = "3620"
'accounts(1) = "3621"
accounts(0) = "3713"
accounts(1) = "3650"
'accounts(4) = "3419"
'accounts(6) = "3690"
'accounts(2) = "3630"

For Each account In accounts
    excel.Visible = True

    On Error Resume Next

    excel.Run "'\\Gateway\hetco\P003\Tasks\PCSPAN\Span4\Automation\DailyReportsUk.xlsm'!GenerateDailyReports" & account
Next

For Each account In accounts
    excel.Visible = True

    On Error Resume Next

    excel.Run "'\\Gateway\hetco\P003\Tasks\PCSPAN\Span4\Automation\DailyReportsRequirementUk.xlsm'!GenerateDailyReports" & account
Next

excel.Quit
Set excel = Nothing