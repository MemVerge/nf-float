params.s3_ref = "s3://mmce-data/workloads/cromwell/bwa/ref/reference.fa"
params.s3_reads = "s3://mmce-data/workloads/cromwell/bwa/data/reads.fq"
params.s3_out = "s3://mmce-data/workloads/cromwell/bwa/out/aln"

process bwaAlign {
  executor = 'float'
  image = 'bwa'

  input:
    val s3_ref
    val s3_reads
    val s3_out

  output:
    stdout

  shell:
  '''
  /opt/aws/dist/aws s3 cp !{s3_ref} reference.fa
  /opt/aws/dist/aws s3 cp !{s3_reads} reads.fq

  mkdir ref
  bwa index reference.fa -p ref/the_ref
  bwa aln -I -t 8 ref/the_ref reads.fq > aln.sai
  bwa samse ref/the_ref aln.sai reads.fq | tee aln.sam

  /opt/aws/dist/aws s3 cp aln.sai !{s3_out}.sai
  /opt/aws/dist/aws s3 cp aln.sam !{s3_out}.sam

  rm reference.fa reads.fq aln.sai aln.sam
  rm -rf ref
  '''
}

workflow {
  bwaAlign(params.s3_ref, params.s3_reads, params.s3_out) | view { it.trim() }
}