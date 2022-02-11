from drm4g.api import Job, DRM4G
job = Job()
job.set_name( "Template sleep" )
job.set_executable( "/bin/sleep" )
job.set_arguments( "1" )
job.set_np( 1 )

drm4g = DRM4G()
job_id = drm4g.submit( job )
print(job_id)


status = drm4g.status(job_id)
print(status)