#!/usr/bin/env python3
"""Windows terminal: F2 checks both gloves before starting, then stops recording over UART.

Run: python tools/glove_serial_control.py --port COM22
Install dependency: python -m pip install pyserial
Focus this terminal before pressing F2. Ctrl+C exits without stopping recording.
"""
import argparse
import base64
import queue
import re
import subprocess
import sys
import threading
import time

ACK=re.compile(r'\[REC_ACK\] (\d+) (RECORDING|STOPPED) ([0-9a-fA-F]{16})')
IMU_CHECK_ACK=re.compile(r'\[IMU_CHECK_ACK\]\s+(\d+)\s+([0-9a-fA-F]{3})\s+'
                        r'([0-9a-fA-F]{3})\s+([01])\s+'
                        r'([0-9a-fA-F]{3})\s+([0-9a-fA-F]{3})')
ALL_IMUS=(1<<11)-1

def describe_mask(mask):
    missing=[f'CH{channel}' for channel in range(11) if not mask & (1<<channel)]
    return '11/11' if not missing else f'{11-len(missing)}/11，缺少'+','.join(missing)

class Speaker:
    """Serialize Windows speech in a worker so UART reception never waits."""
    def __init__(self, enabled=True):
        self.enabled=enabled and sys.platform=='win32'
        self.messages=queue.Queue()
        self.thread=None
        if self.enabled:
            self.thread=threading.Thread(target=self._run,daemon=True)
            self.thread.start()

    def say(self, active):
        if self.enabled:self.messages.put('开始采集' if active else '结束采集')

    def _run(self):
        while True:
            message=self.messages.get()
            try:
                if message is None:return
                # The only interpolated text is one of the two fixed phrases above.
                script=("$ErrorActionPreference='Stop'; Add-Type -AssemblyName System.Speech; "
                        "$voice=New-Object System.Speech.Synthesis.SpeechSynthesizer; "
                        "try { $voice.SelectVoiceByHints([System.Speech.Synthesis.VoiceGender]::NotSet, "
                        "[System.Speech.Synthesis.VoiceAge]::NotSet, 0, "
                        "[System.Globalization.CultureInfo]::GetCultureInfo('zh-CN')) } catch {}; "
                        "try { $voice.SetOutputToDefaultAudioDevice(); "
                        f"$voice.Speak('{message}') "
                        "} finally { $voice.Dispose() }")
                encoded=base64.b64encode(script.encode('utf-16-le')).decode('ascii')
                result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive',
                                       '-EncodedCommand',encoded],capture_output=True,
                                      creationflags=subprocess.CREATE_NO_WINDOW,timeout=20)
                if result.returncode:
                    print('电脑语音播报失败，请检查默认音频输出及 Windows 中文语音组件。',flush=True)
            except (OSError,subprocess.TimeoutExpired):
                print('电脑语音播报失败，串口控制继续正常运行。',flush=True)
            finally:self.messages.task_done()

    def close(self):
        if self.thread:
            self.messages.put(None)
            self.thread.join(timeout=3)

def main():
    parser=argparse.ArgumentParser(description='F2 自检双手 22 路 IMU 后开始/停止录制，F3 查询状态，Ctrl+C 退出。')
    parser.add_argument('--port','-p',default='COM22')
    parser.add_argument('--baud','-b',type=int,default=921600)
    parser.add_argument('--command',choices=['status','check','start','stop'],help='发送单条命令后退出（用于诊断）')
    parser.add_argument('--mute',action='store_true',help='关闭电脑开始/结束语音播报')
    opts=parser.parse_args()
    try:import serial
    except ImportError:parser.exit(1,'请先安装依赖：python -m pip install pyserial\n')
    if not opts.command and sys.platform!='win32':parser.error('F2 模式需要 Windows 终端')
    if not opts.command:import msvcrt
    connection=serial.Serial();connection.port=opts.port;connection.baudrate=opts.baud
    connection.timeout=0;connection.write_timeout=1;connection.dtr=False;connection.rts=False
    try:connection.open()
    except serial.SerialException as error:parser.exit(1,f'无法打开 {opts.port}：{error}\n请关闭占用串口的监视器。\n')
    speaker=Speaker(not opts.mute)
    active=None;pending=None;identifier=0;buffer=b'';last_key=0;received_bytes=0
    def send(action):
        nonlocal identifier,pending
        identifier+=1
        packet=f'@REC {identifier} {action}\n'.encode('ascii')
        connection.write(packet);pending=(identifier,packet,time.monotonic(),0)
        print(f'→ {action}（等待设备确认）',flush=True)
    print(f'已连接 {opts.port}。F2 开始前检查左右手 11 路 IMU；录制中按 F2 停止；F3 查询。',flush=True)
    initial_action='CHECK' if opts.command in ('check','start') else (opts.command.upper() if opts.command else 'STATUS')
    send(initial_action)
    try:
        while True:
            data=connection.read(min(connection.in_waiting,16384))
            if data:
                received_bytes+=len(data)
                buffer+=data
                while b'\n' in buffer:
                    line,buffer=buffer.split(b'\n',1)
                    text=line.decode('utf-8',errors='replace')
                    check=IMU_CHECK_ACK.search(text)
                    if check:
                        if not pending or int(check[1])!=pending[0]:continue
                        action=pending[1].decode('ascii').split()[2]
                        if action!='CHECK':continue
                        pending=None
                        left_detected,left_live=int(check[2],16),int(check[3],16)
                        right_online=check[4]=='1'
                        right_detected,right_live=int(check[5],16),int(check[6],16)
                        healthy=(left_detected==ALL_IMUS and left_live==ALL_IMUS and
                                 right_online and right_detected==ALL_IMUS and
                                 right_live==ALL_IMUS)
                        print(f'左手 IMU：检测 {describe_mask(left_detected)}，当前读取 {describe_mask(left_live)}',flush=True)
                        if right_online:
                            print(f'右手 IMU：检测 {describe_mask(right_detected)}，当前读取 {describe_mask(right_live)}',flush=True)
                        else:
                            print('右手 IMU：未收到自检回复，请检查右手供电、ESP-NOW连接和固件版本。',flush=True)
                        if healthy:
                            print('双手 22 路 IMU 自检通过。',flush=True)
                            if opts.command=='check':return 0
                            send('START')
                        else:
                            print('自检未通过，已阻止开始录制。修复缺失通道后再按 F2。',flush=True)
                            active=False
                            if opts.command:return 1
                        continue
                    match=ACK.search(text)
                    if match:
                        if not pending or int(match[1])!=pending[0]:continue
                        action=pending[1].decode('ascii').split()[2]
                        active=match[2]=='RECORDING';pending=None
                        print(f'← {"正在采集" if active else "已停止采集"} · session={match[3]}',flush=True)
                        if action in ('START','STOP'):speaker.say(active)
                        if opts.command:return 0
                    elif any(term in text for term in ('glove_recording:','serial_record','丢弃','传输失败')):
                        print(text.strip(),flush=True)
                        if '停止录制' in text:active=False
                        elif '开始录制' in text:active=True
                if len(buffer)>65536:buffer=b''
            now=time.monotonic()
            if pending and now-pending[2]>2:
                ident,packet,_,retries=pending
                if retries>=2:
                    action=packet.decode('ascii').split()[2]
                    if action=='CHECK':
                        print('未收到双手 IMU 自检确认，已阻止开始录制。请确认右手已更新固件并在线，再按 F2 重试。',flush=True)
                    else:
                        print('未收到确认，状态未知。检查固件和串口，按 F3 重查。',flush=True)
                    if received_bytes==0:
                        print('串口没有收到任何字节：请检查左手供电、USB 数据线和端口号，按一次 EN/RESET 后再按 F3。',flush=True)
                    pending=None;active=False if action=='CHECK' else None
                    if opts.command:return 1
                else:
                    print(f'确认超时，正在重试 {retries+1}/2…',flush=True)
                    connection.write(packet);pending=(ident,packet,now,retries+1)
            if not opts.command and msvcrt.kbhit():
                key=msvcrt.getwch()
                if key in ('\x00','\xe0'):
                    code=msvcrt.getwch()
                    if code in ('<','=') and now-last_key>.4:
                        last_key=now
                        if pending:print('请等待上一条命令确认。',flush=True)
                        elif code=='=' or active is None:send('STATUS')
                        else:send('STOP' if active else 'CHECK')
            time.sleep(.01)
    except KeyboardInterrupt:
        print('\n已退出串口控制，设备录制状态保持不变；如正在录制，请重新运行后按 F2 停止。')
    except serial.SerialException as error:
        print(f'串口断开：{error}。录制状态未知，请重新连接并查询。');return 1
    finally:
        connection.close()
        speaker.close()
    return 0

if __name__=='__main__':sys.exit(main())
