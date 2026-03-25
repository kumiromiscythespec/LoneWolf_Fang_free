' BUILD_ID: 2026-03-25_free_gui_vbs_launcher_v1
Option Explicit

Dim shell
Dim fso
Dim root
Dim cmdPath
Dim command

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

root = fso.GetParentFolderName(WScript.ScriptFullName)
cmdPath = fso.BuildPath(root, "Launch_LoneWolf_Fang_Free_GUI.cmd")

If Not fso.FileExists(cmdPath) Then
    MsgBox "Launch_LoneWolf_Fang_Free_GUI.cmd was not found.", 16, "LoneWolf Fang Free"
    WScript.Quit 1
End If

command = Chr(34) & cmdPath & Chr(34)
shell.Run command, 0, False
