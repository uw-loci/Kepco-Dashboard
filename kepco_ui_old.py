import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import socket
import math
import csv

class KepcoController:
    def __init__(self):
        self.sock = None
        self.ip_address = ""
        self.port = 5025
        self.buffer_size = 1024
        self.connected = False

    def connect(self, ip, port=5025):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5)
            self.sock.connect((ip, port))
            self.ip_address = ip
            self.port = port
            self.connected = True
            return True, "Connected successfully."
        except Exception as e:
            self.connected = False
            return False, str(e)

    def disconnect(self):
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
        self.connected = False

    def send_scpi(self, command, query=False):
        if not self.connected:
            return None
        
        try:
            full_cmd = command + "\n"
            self.sock.sendall(full_cmd.encode('ascii'))
            if query:
                response = self.sock.recv(self.buffer_size).decode('ascii').strip()
                return response
            return True
        except Exception as e:
            return None

    def upload_waveform(self, points, dwell_time, mode="VOLT"):
        if not self.connected:
            return False, "Not connected"

        try:
            # 1. Reset and Prepare
            self.send_scpi("*CLS")
            self.send_scpi(f"FUNC:MODE {mode}")
            self.send_scpi("LIST:CLE") 

            # 2. Upload Voltage/Current Points (Chunked)
            # Max buffer ~253 chars.
            current_chunk = []
            cmd_prefix = f"LIST:{mode} "
            
            for pt in points:
                val_str = f"{pt:.4f}" # 4 decimal places for precision
                # Check length: prefix + current_data + new_val + comma
                current_len = len(cmd_prefix) + len(",".join(current_chunk)) + len(val_str) + 1
                
                if current_len > 240: # Safe margin below 253
                    self.send_scpi(cmd_prefix + ",".join(current_chunk))
                    current_chunk = []
                
                current_chunk.append(val_str)

            if current_chunk:
                self.send_scpi(cmd_prefix + ",".join(current_chunk))

            # 3. Set Dwell Time
            # Optimization: If all steps have the same duration, we can just send one value 
            # and it applies to all steps (according to manual Appendix B.36).
            # This is much faster than sending a list of identical times.
            self.send_scpi(f"LIST:DWEL {dwell_time:.5f}")

            return True, f"Uploaded {len(points)} points @ {dwell_time:.5f}s"

        except Exception as e:
            return False, str(e)

    def run_waveform(self, mode="VOLT", count=0):
        try:
            # LIST:COUN 0 means infinite loop
            self.send_scpi(f"LIST:COUN {count}") 
            self.send_scpi("OUTP ON")
            self.send_scpi(f"{mode}:MODE LIST") 
            return True, "Waveform Running"
        except Exception as e:
            return False, str(e)

    def stop_output(self):
        try:
            self.send_scpi("OUTP OFF")
            self.send_scpi("FUNC:MODE VOLT") # Revert to fixed mode
            return True, "Output Off"
        except Exception as e:
            return False, str(e)

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Kepco 802E Waveform Generator (High Speed)")
        self.kepco = KepcoController()
        self.setup_ui()

    def setup_ui(self):
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.root, text="Connection")
        conn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(conn_frame, text="IP:").pack(side="left", padx=5)
        self.ip_entry = ttk.Entry(conn_frame, width=15)
        self.ip_entry.insert(0, "192.168.1.10") 
        self.ip_entry.pack(side="left", padx=5)
        
        self.connect_btn = ttk.Button(conn_frame, text="Connect", command=self.toggle_connect)
        self.connect_btn.pack(side="left", padx=5)
        self.status_lbl = ttk.Label(conn_frame, text="Disconnected", foreground="red")
        self.status_lbl.pack(side="left", padx=10)

        # Config Frame
        config_frame = ttk.LabelFrame(self.root, text="Waveform Configuration")
        config_frame.pack(fill="x", padx=10, pady=5)

        # Inputs
        ttk.Label(config_frame, text="Type:").grid(row=0, column=0, padx=5, pady=5)
        self.wave_type = ttk.Combobox(config_frame, values=["Sine", "Square", "Triangle", "Sawtooth", "CSV Custom"])
        self.wave_type.current(0)
        self.wave_type.grid(row=0, column=1, padx=5, pady=5)
        self.wave_type.bind("<<ComboboxSelected>>", self.on_wave_change)

        ttk.Label(config_frame, text="Freq (Hz):").grid(row=1, column=0, padx=5, pady=5)
        self.freq_entry = ttk.Entry(config_frame)
        self.freq_entry.insert(0, "40.0") # Default to high freq test
        self.freq_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(config_frame, text="Amp (V/A):").grid(row=2, column=0, padx=5, pady=5)
        self.amp_entry = ttk.Entry(config_frame)
        self.amp_entry.insert(0, "10.0")
        self.amp_entry.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(config_frame, text="Offset (V/A):").grid(row=3, column=0, padx=5, pady=5)
        self.offset_entry = ttk.Entry(config_frame)
        self.offset_entry.insert(0, "0.0")
        self.offset_entry.grid(row=3, column=1, padx=5, pady=5)

        # Mode Selection
        ttk.Label(config_frame, text="Mode:").grid(row=4, column=0, padx=5, pady=5)
        self.mode_var = tk.StringVar(value="VOLT")
        ttk.Radiobutton(config_frame, text="Voltage", variable=self.mode_var, value="VOLT").grid(row=4, column=1, sticky="w")
        ttk.Radiobutton(config_frame, text="Current", variable=self.mode_var, value="CURR").grid(row=4, column=2, sticky="w")

        # CSV
        self.csv_btn = ttk.Button(config_frame, text="Load CSV", command=self.load_csv)
        self.csv_lbl = ttk.Label(config_frame, text="No file")

        # Controls
        ctrl_frame = ttk.Frame(self.root)
        ctrl_frame.pack(fill="x", padx=10, pady=10)
        ttk.Button(ctrl_frame, text="Upload & Run", command=self.run_sequence).pack(side="left", padx=5)
        ttk.Button(ctrl_frame, text="Stop", command=self.stop_sequence).pack(side="left", padx=5)

        # Log
        self.log_text = tk.Text(self.root, height=10, width=50)
        self.log_text.pack(padx=10, pady=5)

    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def on_wave_change(self, event):
        if self.wave_type.get() == "CSV Custom":
            self.csv_btn.grid(row=0, column=2, padx=5)
            self.csv_lbl.grid(row=0, column=3, padx=5)
        else:
            self.csv_btn.grid_forget()
            self.csv_lbl.grid_forget()

    def toggle_connect(self):
        if not self.kepco.connected:
            ip = self.ip_entry.get()
            success, msg = self.kepco.connect(ip)
            if success:
                self.connect_btn.config(text="Disconnect")
                self.status_lbl.config(text="Connected", foreground="green")
                self.log(f"Connected to {ip}")
            else:
                self.log(f"Fail: {msg}")
        else:
            self.kepco.disconnect()
            self.connect_btn.config(text="Connect")
            self.status_lbl.config(text="Disconnected", foreground="red")
            self.log("Disconnected")

    def load_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.csv_path = path
            self.csv_lbl.config(text=path.split("/")[-1])
            self.log(f"CSV: {path}")

    def generate_points(self):
        w_type = self.wave_type.get()
        try:
            freq = float(self.freq_entry.get())
            amp = float(self.amp_entry.get())
            offset = float(self.offset_entry.get())
        except ValueError:
            messagebox.showerror("Error", "Invalid numeric input")
            return None, None

        # --- Updated Timing Calculation ---
        # Manual says MIN DWELL is 0.0005s (500us).
        # We target a reasonable number of points (e.g., 50-100)
        # providing it doesn't violate the 0.0005s limit.
        
        target_points = 100
        period = 1.0 / freq
        calculated_dwell = period / target_points
        
        # Enforce Minimum Dwell Limit (0.0005s)
        if calculated_dwell < 0.0005:
            # If freq is too high for 100 points, reduce point count
            # Set dwell to minimum and calculate max possible points
            calculated_dwell = 0.0005
            target_points = int(period / calculated_dwell)
            if target_points < 2:
                 messagebox.showerror("Error", f"Frequency {freq}Hz is too high even for min dwell!")
                 return None, None
        
        # Recalculate exact dwell to keep frequency accurate with integer points
        dwell_time = period / target_points
        
        self.log(f"Gen: {w_type}, {target_points} pts, Dwell: {dwell_time:.5f}s")
        
        points = []
        if w_type == "Sine":
            for i in range(target_points):
                angle = 2 * math.pi * (i / target_points)
                val = offset + (amp * math.sin(angle))
                points.append(val)
        
        elif w_type == "Square":
            for i in range(target_points):
                val = offset + amp if i < target_points/2 else offset - amp
                points.append(val)

        elif w_type == "Triangle":
            half = target_points // 2
            step = (2 * amp) / half
            start = offset - amp
            for i in range(target_points):
                if i <= half:
                    points.append(start + (step * i))
                else:
                    points.append((offset + amp) - (step * (i - half)))

        elif w_type == "Sawtooth":
            step = (2 * amp) / (target_points - 1)
            start = offset - amp
            for i in range(target_points):
                points.append(start + (step * i))

        elif w_type == "CSV Custom":
            if not getattr(self, 'csv_path', None):
                messagebox.showerror("Error", "Load CSV first")
                return None, None
            try:
                with open(self.csv_path, 'r') as f:
                    reader = csv.reader(f)
                    data = [float(x) for row in reader for x in row]
                    # Resample to target_points or just take first N
                    # Simple approach: Take first N up to limit, adjust dwell
                    points = data[:1000] # Hard limit of device
                    target_points = len(points)
                    dwell_time = period / target_points
                    if dwell_time < 0.0005:
                         messagebox.showwarning("Warning", "CSV too long for freq. Truncating.")
                         max_pts = int(period / 0.0005)
                         points = points[:max_pts]
                         dwell_time = 0.0005
            except Exception as e:
                messagebox.showerror("CSV Error", str(e))
                return None, None

        return points, dwell_time

    def run_sequence(self):
        if not self.kepco.connected:
            messagebox.showerror("Error", "Connect first")
            return

        data = self.generate_points()
        if not data: return
        points, dwell = data

        mode = self.mode_var.get()
        success, msg = self.kepco.upload_waveform(points, dwell, mode)
        self.log(msg)
        if success:
            res, rmsg = self.kepco.run_waveform(mode)
            self.log(rmsg)

    def stop_sequence(self):
        if self.kepco.connected:
            self.kepco.stop_output()
            self.log("Stopped")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()