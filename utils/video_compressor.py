import tkinter as tk
from tkinter import filedialog
import subprocess


def seleccionar_video():
    ruta_video = filedialog.askopenfilename(title="Seleccionar video")
    entry_video.delete(0, tk.END)
    entry_video.insert(0, ruta_video)


def seleccionar_salida():
    ruta_salida = filedialog.asksaveasfilename(
        defaultextension=".mp4",
        filetypes=[("Archivos de video", "*.mp4")],
        title="Guardar como"
    )
    entry_salida.delete(0, tk.END)
    entry_salida.insert(0, ruta_salida)


def comprimir_video():
    input_path = entry_video.get()
    output_path = entry_salida.get()
    bitrate = entry_bitrate.get() + 'k'

    if not input_path or not output_path:
        lbl_estado.config(text="Por favor, seleccione los archivos.")
        return

    command = [
        'ffmpeg',
        '-i', input_path,
        '-b:v', bitrate,
        '-bufsize', bitrate,
        output_path
    ]

    try:
        subprocess.run(command, check=True)
        lbl_estado.config(text="¡Video comprimido con éxito!")
    except subprocess.CalledProcessError as e:
        lbl_estado.config(text=f"Error al comprimir: {e}")


# GUI
root = tk.Tk()
root.title("Compresión de video")

lbl_video = tk.Label(root, text="Video de entrada:")
lbl_video.grid(row=0, column=0, padx=5, pady=5)
entry_video = tk.Entry(root, width=40)
entry_video.grid(row=0, column=1, padx=5, pady=5)
btn_seleccionar_video = tk.Button(root, text="Seleccionar", command=seleccionar_video)
btn_seleccionar_video.grid(row=0, column=2, padx=5, pady=5)

lbl_salida = tk.Label(root, text="Guardar como:")
lbl_salida.grid(row=1, column=0, padx=5, pady=5)
entry_salida = tk.Entry(root, width=40)
entry_salida.grid(row=1, column=1, padx=5, pady=5)
btn_seleccionar_salida = tk.Button(root, text="Seleccionar", command=seleccionar_salida)
btn_seleccionar_salida.grid(row=1, column=2, padx=5, pady=5)

lbl_bitrate = tk.Label(root, text="Bitrate (k):")
lbl_bitrate.grid(row=2, column=0, padx=5, pady=5)
entry_bitrate = tk.Entry(root, width=10)
entry_bitrate.grid(row=2, column=1, padx=5, pady=5)
entry_bitrate.insert(0, "1000")

btn_comprimir = tk.Button(root, text="Comprimir", command=comprimir_video)
btn_comprimir.grid(row=3, column=1, padx=5, pady=10)

lbl_estado = tk.Label(root, text="")
lbl_estado.grid(row=4, column=1, padx=5, pady=5)

root.mainloop()
