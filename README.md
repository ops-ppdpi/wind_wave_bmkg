# wind_wave_bmkg
## An automatic System to Process Wind-Wave dataset from BMKG-OFS

### url_opendap
address     : http://peta-maritim.bmkg.go.id/opendap/ww3gfs/
username    : brol
passwoord   : ****

### Variables
hs	  : significant wave height (units: m)
uwnd	: wind U – vector (units: knot)
vwnd	: wind V – vector (units: knot)

### Global		1200				0000
time		[6:1:6]				[10:1:10]
lat		[0:1:140]			  [0:1:140]
lon		[0:1:359]			  [0:1:359]

### Reguler		1200				0000
time		[6:1:6]				[10:1:10]
lat		[0:1:120]			  [0:1:120]
lon		[0:1:180]			  [0:1:180]	

### Hires		1200				0000
time		[6:1:6]			[10:1:10]
lat		[0:1:480]			[0:1:480]
lon		[0:1:880]			[0:1:880]

### filename_1200					              filename_0000
w3g_global_yyyymmdd_1200.nc.nc4		  w3g_global_yyyymmdd_0000.nc.nc4
w3g_reg_ yyyymmdd _1200.nc.nc4		  w3g_reg_ yyyymmdd _0000.nc.nc4
w3g_hires_ yyyymmdd _1200.nc.nc4		w3g_hires_ yyyymmdd _0000.nc.nc4

### Directory
sidik_fs : ftp://@fs.bpol.net/sidik/model/ww_bmkg/
