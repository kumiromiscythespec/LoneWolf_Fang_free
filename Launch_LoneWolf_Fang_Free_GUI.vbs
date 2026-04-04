' BUILD_ID: 2026-03-30_free_gui_vbs_native_shim_v1
Option Explicit

Dim shell
Dim fso
Dim root
Dim launcherExe
Dim cmdPath
Dim command

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

root = fso.GetParentFolderName(WScript.ScriptFullName)
launcherExe = fso.BuildPath(root, "LoneWolfFangFreeLauncher.exe")
cmdPath = fso.BuildPath(root, "Launch_LoneWolf_Fang_Free_GUI.cmd")

If fso.FileExists(launcherExe) Then
    command = QuoteArg(launcherExe) & BuildArgumentList(WScript.Arguments)
    shell.Run command, 0, False
    WScript.Quit 0
End If

If Not fso.FileExists(cmdPath) Then
    MsgBox "LoneWolfFangFreeLauncher.exe or Launch_LoneWolf_Fang_Free_GUI.cmd was not found.", 16, "LoneWolf Fang Free"
    WScript.Quit 1
End If

command = QuoteArg(cmdPath) & BuildArgumentList(WScript.Arguments)
shell.Run command, 0, False

Function QuoteArg(value)
    QuoteArg = Chr(34) & Replace(value, Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function

Function BuildArgumentList(args)
    Dim i
    Dim result

    result = ""
    For i = 0 To args.Count - 1
        result = result & " " & QuoteArg(CStr(args.Item(i)))
    Next

    BuildArgumentList = result
End Function
