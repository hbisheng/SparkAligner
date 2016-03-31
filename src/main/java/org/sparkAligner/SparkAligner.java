package org.sparkAligner;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SparkAligner {
    
    static class SeedInfoComparator implements Comparator<BytesWritable>, Serializable {
        @Override
        public int compare(BytesWritable o1, BytesWritable o2) {
            // reference seeds should be seen before query seeds 
            
            BytesWritable bw1 = SparkAligner.copyByteFromBytesWritable( (BytesWritable) o1);
            BytesWritable bw2 = SparkAligner.copyByteFromBytesWritable( (BytesWritable) o2);
            
            byte [] b1 = bw1.getBytes();
            byte [] b2 = bw2.getBytes();
            
            if(b1.length == 0 & b2.length != 0) {
                return 1;
            }
            if(b1.length == 0 & b2.length == 0) {
                return 0;
            }
            if(b1.length != 0 & b2.length == 0) {
                return -1;
            }
            // just compare the first byte
            boolean firstIsQry  = (b1[0]&0x01) == 0;
            boolean secondIsQry = (b2[0]&0x01) == 0;
            
            return Boolean.compare(firstIsQry, secondIsQry);
        }
    }
    
    static class SeedPartitioner extends Partitioner {
        private int numPart = 0; 
        public SeedPartitioner(int num) {
            numPart = num;
        }
        
        @Override
        public int getPartition(Object arg0) {
            BytesWritable key = SparkAligner.copyByteFromBytesWritable( (BytesWritable) arg0 );
            int part = (WritableComparator.hashBytes(key.getBytes(), key.getLength()-1) & Integer.MAX_VALUE) % numPart;
            return part;
        }
    
        @Override
        public int numPartitions() {
            return numPart;
        }
    };
    
    static class SeedComparator implements Comparator<BytesWritable>, Serializable {
        @Override
        public int compare(BytesWritable o1, BytesWritable o2) {
            
            BytesWritable bw1 = SparkAligner.copyByteFromBytesWritable( (BytesWritable) o1);
            BytesWritable bw2 = SparkAligner.copyByteFromBytesWritable( (BytesWritable) o2);
            
            byte [] b1 = bw1.getBytes();
            byte [] b2 = bw2.getBytes();
            
            // skip the last byte which has the ref/qry flag
            int len = bw1.getLength()-1;
            for (int i = 0; i < len; i++)
            {
                int diff = b1[i] - b2[i];
                if (diff != 0) { 
                    return diff; 
                }
            }
            return 0;
        }
    }
    
    static PairFunction<Tuple2<IntWritable, BytesWritable>, IntWritable, BytesWritable> mapToClone 
        = new PairFunction<Tuple2<IntWritable,BytesWritable>, IntWritable, BytesWritable>() {
            @Override
            public Tuple2<IntWritable, BytesWritable> call(Tuple2<IntWritable, BytesWritable> arg0)
                    throws Exception {
                byte[] bytesCopied = new byte[arg0._2.getLength()];
                System.arraycopy(arg0._2.getBytes(), 0, bytesCopied, 0, bytesCopied.length);
                return new Tuple2<IntWritable, BytesWritable>(new IntWritable(arg0._1.get()), new BytesWritable(bytesCopied));
            }
        };
        
    protected static BytesWritable copyByteFromBytesWritable(BytesWritable next) {          
        byte[] bytesCopied = new byte[next.getLength()];
        System.arraycopy(next.getBytes(), 0, bytesCopied, 0, bytesCopied.length);
        return new BytesWritable(bytesCopied);
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length < 3) {
          System.err.println("Usage: SparkAligner ref-path reads-path output-path");
          System.exit(0);
        }
        
        String dataHostURL = "hdfs://localhost:9000";
        String refPath = dataHostURL + args[0];
        String qryPath = dataHostURL + args[1];
        String outputPath = dataHostURL + args[2];
        int refPartition = 6;
        int qryPartition = 6;
        
        final int MIN_READ_LEN = 36;
        final int MAX_READ_LEN = 36;
        final int K = 3;
        final boolean ALLOW_DIFFERENCES = false;
        final boolean FILTER_ALIGNMENTS = true;
        final int MAPPER_NUM = 240;
        final int REDUCER_NUM = 48;
        final int FILTER_MAPPER_NUM = 24;
        final int FILTER_REDUCER_NUM = 24;
        final int BLOCK_SIZE = 128;
        final int REDUNDANCY = 16;
        
        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[12]");    
        conf.registerKryoClasses(
            new Class<?>[] {
                Class.forName("org.apache.hadoop.io.IntWritable"),
                Class.forName("org.apache.hadoop.io.BytesWritable")
            }
        ); 
        JavaSparkContext context = new JavaSparkContext(conf);
        
        long timeStart = System.currentTimeMillis();
        long timeMid = -1;
        
        JavaPairRDD<IntWritable, BytesWritable> refRawSequence = context.sequenceFile(refPath, IntWritable.class, BytesWritable.class, refPartition);
        JavaPairRDD<IntWritable, BytesWritable> qryRawSequence = context.sequenceFile(qryPath, IntWritable.class, BytesWritable.class, qryPartition);
        
        JavaPairRDD<IntWritable, BytesWritable> clonedRefSequence = refRawSequence.mapToPair(mapToClone);
        JavaPairRDD<IntWritable, BytesWritable> clonedQrySequence = qryRawSequence.mapToPair(mapToClone);
        
        JavaPairRDD<BytesWritable, BytesWritable> mappedRefSeeds = clonedRefSequence
            .flatMapToPair(
                /**
                 *  Map the reference sequence to seeds
                 */
                new PairFlatMapFunction<Tuple2<IntWritable, BytesWritable>, BytesWritable, BytesWritable>(){
                    boolean ISREF = true;
                    int CHUNK_OVERLAP = 1024;
                    int SEED_LEN   = MIN_READ_LEN / (K+1);
                    int FLANK_LEN  = MAX_READ_LEN - SEED_LEN + K; 
                    
                    @Override
                    public Iterable<Tuple2<BytesWritable, BytesWritable>> call(
                            Tuple2<IntWritable, BytesWritable> seqTuple) throws Exception {
                        
                        List<Tuple2<BytesWritable, BytesWritable>> res = new ArrayList<Tuple2<BytesWritable, BytesWritable>>(); 
                        BytesWritable rawRecord = copyByteFromBytesWritable(seqTuple._2);
                        
                        MerRecord seedInfo = new MerRecord();
                        byte [] seedbuffer   = new byte[DNAString.arrToSeedLen(SEED_LEN, REDUNDANCY)];
                        
                        FastaRecord record = new FastaRecord();
                        record.fromBytes(rawRecord);
                        byte [] seq         = record.m_sequence;
                        int realoffsetstart = record.m_offset;
                        boolean isLast      = record.m_lastChunk;
                        
                        BytesWritable seed = new BytesWritable();
                        seedInfo.id          = seqTuple._1.get();
                        seedInfo.isReference = ISREF;
                        seedInfo.isRC        = false;
                        int seqlen = seq.length;
                        int startoffset = 0;
                        if (realoffsetstart != 0) { 
                            // If I'm not the first chunk, shift over so there is room for the left flank
                            startoffset = CHUNK_OVERLAP + 1 - FLANK_LEN - SEED_LEN;
                            realoffsetstart += startoffset;
                        }
                        // stop so the last mer will just fit
                        int end = seqlen - SEED_LEN + 1;
                        if (!isLast) {
                            // if I'm not the last chunk, stop so the right flank will fit as well
                            end -= FLANK_LEN;
                        }
                        for (int start = startoffset, realoffset = realoffsetstart; start < end; start++, realoffset++) {
                            // emit the mers starting at every position in the range
                            if (DNAString.arrHasN(seq, start, SEED_LEN)) { continue; } // don't bother with seeds with n's
                            
                            seedInfo.offset = realoffset;
                            // figure out the ranges for the flanking sequence
                            int leftstart = start-FLANK_LEN;
                            if (leftstart < 0) { leftstart = 0; }
                            int leftlen = start-leftstart;
                            
                            int rightstart = start+SEED_LEN;
                            int rightend = rightstart + FLANK_LEN;
                            if (rightend > seqlen) { rightend = seqlen; }
                            int rightlen = rightend-rightstart;
                            
                            BytesWritable seedbinary = seedInfo.toBytes(seq, leftstart, leftlen, rightstart, rightlen);
                            if ((REDUNDANCY > 1) && (DNAString.repseed(seq, start, SEED_LEN))) {
                                for (int r = 0; r < REDUNDANCY; r++) {
                                    DNAString.arrToSeed(seq, start, SEED_LEN, seedbuffer, 0, r, REDUNDANCY, 0);
                                    seed.set(seedbuffer, 0, seedbuffer.length);
                                    res.add(new Tuple2<BytesWritable, BytesWritable>(copyByteFromBytesWritable(seed), copyByteFromBytesWritable(seedbinary)));
                                }
                            }
                            else {
                                DNAString.arrToSeed(seq, start, SEED_LEN, seedbuffer, 0, 0, REDUNDANCY, 0);
                                seed.set(seedbuffer, 0, seedbuffer.length);
                                res.add(new Tuple2<BytesWritable, BytesWritable>(copyByteFromBytesWritable(seed), copyByteFromBytesWritable(seedbinary)));
                            }
                        }
                        return res;
                    }
                });
    
        
        JavaPairRDD<BytesWritable, BytesWritable> mappedQrySeeds = clonedQrySequence
            .flatMapToPair(
                /**
                 * Map the query sequence to seeds
                 */
                new PairFlatMapFunction<Tuple2<IntWritable, BytesWritable>, BytesWritable, BytesWritable>(){
                    
                    boolean ISREF = false;
                    int CHUNK_OVERLAP = 1024;
                    int SEED_LEN   = MIN_READ_LEN / (K+1);
                    int FLANK_LEN  = MAX_READ_LEN - SEED_LEN + K; 
                    
                    @Override
                    public Iterable<Tuple2<BytesWritable, BytesWritable>> call(
                            Tuple2<IntWritable, BytesWritable> seqTuple) throws Exception {
                        BytesWritable rawRecord = copyByteFromBytesWritable(seqTuple._2);
                        List<Tuple2<BytesWritable, BytesWritable>> res = new ArrayList<Tuple2<BytesWritable, BytesWritable>>(); 
                        
                        FastaRecord record = new FastaRecord();
                        record.fromBytes(rawRecord);
                        
                        byte [] seq         = record.m_sequence;
                        int seqlen = seq.length;
                        if (seqlen < MIN_READ_LEN) {
                            throw new IOException("ERROR: seqlen=" + seqlen + " < MIN_READ_LEN=" + MIN_READ_LEN + " in reads file!");
                        }
                        if (seqlen > MAX_READ_LEN) {
                            throw new IOException("ERROR: seqlen=" + seqlen + " > MAX_READ_LEN=" + MAX_READ_LEN + " in reads file!");
                        }
                        
                        int numN = 0;
                        for (int i = 0; i < seqlen; i++) {
                            if (seq[i] == 'N') { numN++; }
                        }
                        if (numN > K) { return res; }

                        for (int rc = 0; rc < 2; rc++) {
                            
                            MerRecord seedInfo = new MerRecord();
                            seedInfo.id          = seqTuple._1.get();
                            seedInfo.isReference = ISREF;
                            seedInfo.isRC        = false;
                            
                            // reverse complement the sequence
                            if (rc == 1) {
                                DNAString.rcarr_inplace(seq);
                                seedInfo.isRC = true;
                            }

                            // only emit the non-overlapping mers
                            for (int i = 0; i + SEED_LEN <= seqlen; i += SEED_LEN) {
                                if (DNAString.arrHasN(seq, i, SEED_LEN)) { continue; }
                                
                                byte [] seedbuffer   = new byte[DNAString.arrToSeedLen(SEED_LEN, REDUNDANCY)];
                                if ((REDUNDANCY > 1) && (DNAString.repseed(seq, i, SEED_LEN))) {
                                    DNAString.arrToSeed(seq, i, SEED_LEN, seedbuffer, 0, seedInfo.id, REDUNDANCY, 0);   
                                } else {
                                    DNAString.arrToSeed(seq, i, SEED_LEN, seedbuffer, 0, 0, REDUNDANCY, 0);
                                }
                                
                                BytesWritable seedBinary = new BytesWritable();
                                seedBinary.set(seedbuffer, 0, seedbuffer.length);
                                seedInfo.offset = i;
                                
                                // figure out the ranges for the flanking sequence
                                int leftstart = 0;
                                int leftlen = i;
                                
                                int rightstart = i + SEED_LEN;
                                int rightlen = seqlen-rightstart;
                                
                                BytesWritable seedInfoBinary = seedInfo.toBytes(seq, leftstart, leftlen, rightstart, rightlen);
                                res.add(new Tuple2<BytesWritable, BytesWritable>(copyByteFromBytesWritable(seedBinary), copyByteFromBytesWritable(seedInfoBinary)));
                            }
                        }
                        return res;
                    }
                });
        
        JavaPairRDD<BytesWritable, BytesWritable> combinedMappedSeeds = mappedQrySeeds.union(mappedRefSeeds);
        
        JavaPairRDD<BytesWritable, Iterable<BytesWritable>> groupedSeeds = 
            combinedMappedSeeds.groupByKey().mapValues(
                /**
                 * Sort the seeds to let references seeds come before query seeds
                 */
                new Function<Iterable<BytesWritable>, Iterable<BytesWritable>>() {
                    @Override
                    public Iterable<BytesWritable> call(Iterable<BytesWritable> arg0)
                            throws Exception {
                        
                        // sort the record by value
                        List<BytesWritable> res = new ArrayList<BytesWritable>();
                        Iterator<BytesWritable> iter = arg0.iterator();
                        while(iter.hasNext()) {
                            res.add( copyByteFromBytesWritable((iter.next())) );
                        }
                        //Collections.sort(res, new SeedInfoComparator());
                        return res;
                    }
                });
        
        JavaPairRDD<IntWritable, BytesWritable> alignments = 
            groupedSeeds.flatMapToPair(
                /**
                 * For each seeds, gather their record and perform batch align  
                 */
                new PairFlatMapFunction<Tuple2<BytesWritable, Iterable<BytesWritable>>, IntWritable, BytesWritable>() {         
                    
                    int SEED_LEN   = MIN_READ_LEN / (K+1);
                    
                    @Override
                    public Iterable<Tuple2<IntWritable, BytesWritable>> call(
                            Tuple2<BytesWritable, Iterable<BytesWritable>> iterSeedTuple)
                            throws Exception {
                        
                        List<Tuple2<IntWritable, BytesWritable>> res = new ArrayList<Tuple2<IntWritable, BytesWritable>>();
                        List<MerRecord> reftuples = new ArrayList<MerRecord>();
                        List<MerRecord> qrytuples = new ArrayList<MerRecord>();
                        
                        Iterator<BytesWritable> iterForSeedInfo = iterSeedTuple._2.iterator();
                        
                        int totalr = 0;
                        int totalq = 0;
                        int qbatch = 0;
                        // Reference mers are first, save them away
                        while (iterForSeedInfo.hasNext()) {
                            BytesWritable btmp = iterForSeedInfo.next();
                            if(btmp.getLength() == 0) continue;

                            MerRecord merIn = new MerRecord(copyByteFromBytesWritable(btmp)); 
                            if (merIn.isReference) {
                                // just save away the reference tuples
                                totalr++;
                                reftuples.add(merIn);
                                if (totalq != 0) {
                                    //String ss = DNAString.bytesToString(DNAString.seedToArr(mer.get(), SEED_LEN, REDUNDANCY));
                                    return new ArrayList<Tuple2<IntWritable, BytesWritable>>();
                                    //throw new IOException("ERROR: Saw a reference seed after a query seed");
                                }
                            }   
                            else {
                                if (totalr == 0) {
                                    // got a qry tuple, but there were no reference tuples
                                    // System.err.println(" Saw a query tuple, but no referernce tuple!!!");
                                    return new ArrayList<Tuple2<IntWritable, BytesWritable>>();
                                }
        
                                qrytuples.add(merIn);
                                totalq++;
                                qbatch++;
                                
                                if (qbatch == BLOCK_SIZE) {
                                    alignBatch(res, reftuples, qrytuples);
                                    qrytuples.clear();
                                    qbatch = 0;
                                }
                            }
                        }
                        
                        if (qbatch != 0){
                            alignBatch(res, reftuples, qrytuples);
                        }
                        
                        return res;
                    }
                    
                    public void alignBatch(List<Tuple2<IntWritable, BytesWritable>> res, List<MerRecord> reftuples, List<MerRecord> qrytuples) 
                            throws IOException
                    {
                        AlignmentRecord [] bestalignments = null;
                        AlignmentRecord [] secondalignments = null;
                        boolean [] recordsecond;
                        int [] bestk;
                        
                        if (FILTER_ALIGNMENTS) {
                            bestalignments   = new AlignmentRecord[BLOCK_SIZE];
                            secondalignments = new AlignmentRecord[BLOCK_SIZE];
                            recordsecond     = new boolean[BLOCK_SIZE];
                            bestk            = new int[BLOCK_SIZE];
                            for (int i = 0; i < BLOCK_SIZE; i++)
                            {
                                bestalignments[i]   = new AlignmentRecord();
                                secondalignments[i] = new AlignmentRecord();
                            }
                        }
                        
                        int numr = reftuples.size();
                        int numq = qrytuples.size();
                        
                        // join together the query-ref shared mers
                        if ((numr != 0) && (numq != 0)) {       
                            // Align reads to the references in blocks of BLOCK_SIZE x BLOCK_SIZE to improve cache locality
                            // define a qry block between [startq, lastq)
                            
                            for (int startq = 0; startq < numq; startq += BLOCK_SIZE) {
                                int lastq = startq + BLOCK_SIZE;
                                if (lastq > numq) { lastq = numq; }
                                
                                if (FILTER_ALIGNMENTS) {
                                  java.util.Arrays.fill(bestk, K+1);
                                }
                                
                                // define a ref block between [startr, lastr)
                                for (int startr = 0; startr < numr; startr += BLOCK_SIZE) {
                                    int lastr = startr + BLOCK_SIZE;
                                    if (lastr > numr) { lastr = numr; }
        
                                    // for each element in [startq, lastq)
                                    for (int curq = startq; curq < lastq; curq++) {
                                        MerRecord qry = qrytuples.get(curq);
                                        
                                        // for each element in [startr, lastr)
                                        for (int curr = startr; curr < lastr; curr++) {
                                            AlignmentRecord rec = extend(qry, reftuples.get(curr));
                                            
                                            if (rec.m_differences == -1) continue;
                                            if (FILTER_ALIGNMENTS) {
                                                int qidx = curq - startq;
                                                if (rec.m_differences < bestk[qidx]) { 
                                                    bestk[qidx] = rec.m_differences;
                                                    bestalignments[qidx].set(rec);
                                                    recordsecond[qidx] = false;
                                                } else if (rec.m_differences == bestk[qidx]) {  
                                                    secondalignments[qidx].set(rec);
                                                    recordsecond[qidx] = true;
                                                }
                                            } else {
                                                res.add(new Tuple2<IntWritable, BytesWritable>(new IntWritable(qry.id), rec.toBytes()));
                                            }
                                        }
                                    }
                                }
                                
                                if (FILTER_ALIGNMENTS) {
                                    for (int qidx = 0; qidx < lastq - startq; qidx++) {
                                        if (bestk[qidx] <= K) {
                                            res.add(new Tuple2<IntWritable, BytesWritable>(new IntWritable(qrytuples.get(qidx+startq).id), bestalignments[qidx].toBytes()));
                                            if (recordsecond[qidx]) {
                                                res.add(new Tuple2<IntWritable, BytesWritable>(new IntWritable(qrytuples.get(qidx+startq).id), secondalignments[qidx].toBytes()));
                                            }
                                        }
                                    }
                                }
                                
                            }
                        }
        
                    }
                    
                    //------------------------- extend --------------------------
                    // Given an exact shared seed, try to extend to a full length alignment
                    public AlignmentRecord extend(MerRecord qrytuple, MerRecord reftuple) throws IOException 
                    {
                        int refStart    = reftuple.offset;
                        int refEnd      = reftuple.offset + SEED_LEN;
                        int differences = 0;
                        LandauVishkin landauVishkinObj = new LandauVishkin();
                        landauVishkinObj.configure(K);
                        
                        try {               
                            if (qrytuple.leftFlank.length != 0) {
                                // at least 1 read base on the left needs to be aligned
                                int realleftflanklen = DNAString.dnaArrLen(qrytuple.leftFlank);
                                
                                // aligned the pre-reversed strings!
                                AlignInfo a = landauVishkinObj.extend(
                                        reftuple.leftFlank, 
                                        qrytuple.leftFlank, 
                                        K, ALLOW_DIFFERENCES);
                                
                                if (a.alignlen == -1) { return new AlignmentRecord(-1, -1, -1, -1, true); } // alignment failed
                                if (!a.isBazeaYatesSeed(realleftflanklen, SEED_LEN)) { return new AlignmentRecord(-1, -1, -1, -1, true); }
                                
                                refStart    -= a.alignlen;
                                differences = a.differences;
                            }
                            
                            if (qrytuple.rightFlank.length != 0) {
                                AlignInfo b = landauVishkinObj.extend(reftuple.rightFlank, 
                                                                               qrytuple.rightFlank, 
                                                                               K - differences, 
                                                                               ALLOW_DIFFERENCES);
                                if (b.alignlen == -1) { return new AlignmentRecord(-1, -1, -1, -1, true);   } // alignment failed
                                refEnd      += b.alignlen;
                                differences += b.differences;
                            }
                            
                            AlignmentRecord fullalignment = new AlignmentRecord();
                            fullalignment.m_refID       = reftuple.id;
                            fullalignment.m_refStart    = refStart;
                            fullalignment.m_refEnd      = refEnd;
                            fullalignment.m_differences = differences;
                            fullalignment.m_isRC        = qrytuple.isRC;
                            return fullalignment;
                        }
                        catch (Exception e) {
                            //throw new IOException("Problem with read:" + qrytuple.id + " :" + e.getMessage() + "\n");   
                            //System.out.println("Problem with read:" + qrytuple.id + ":" + e.toString());
                            return new AlignmentRecord(-1, -1, -1, -1, true);
                        }
                    }
                }); 
        
        alignments.saveAsNewAPIHadoopFile(
                outputPath+"-alignments", 
                IntWritable.class, 
                BytesWritable.class, 
                SequenceFileOutputFormat.class);
        
        timeMid = System.currentTimeMillis();
        System.out.println("Alignment time: " + (timeMid-timeStart)/1000.0 + " seconds");
        
        //********************************************RESULTS FILTERING************************************************************
        JavaPairRDD<IntWritable, BytesWritable> clonedAlignments 
            = context.sequenceFile(outputPath+"-alignments/part*", IntWritable.class, BytesWritable.class, refPartition + qryPartition).mapToPair(mapToClone);
        
        JavaPairRDD<IntWritable, Iterable<BytesWritable>> mappedAlignments = clonedAlignments.mapToPair(
            /**
             * Add a list to each record value
             */
            new PairFunction<Tuple2<IntWritable, BytesWritable>, IntWritable, Iterable<BytesWritable>>() {
                @Override
                public Tuple2<IntWritable, Iterable<BytesWritable>> call(
                        Tuple2<IntWritable, BytesWritable> arg) throws Exception {
                    return new Tuple2<IntWritable, Iterable<BytesWritable>>(arg._1, Arrays.asList(arg._2));
                }
        });
        
        JavaPairRDD<IntWritable, BytesWritable> unambiguousAlign 
            = mappedAlignments.reduceByKey(
                /**
                 * merge record values and leave the best two   
                 */
                new Function2<Iterable<BytesWritable>, Iterable<BytesWritable>, Iterable<BytesWritable>>() {
                    @Override
                    public Iterable<BytesWritable> call(Iterable<BytesWritable> BytesListOne,
                            Iterable<BytesWritable> BytesListTwo) throws Exception {        
                        ArrayList<BytesWritable> res = new ArrayList<BytesWritable>();
                        
                        AlignmentRecord bestAlignment = new AlignmentRecord();
                        AlignmentRecord secondBestAlignment = null;
                        AlignmentRecord curAlignment = new AlignmentRecord();
                        
                        Iterator<BytesWritable> iterOne = BytesListOne.iterator();
                        bestAlignment.fromBytes( iterOne.next());
                        while(iterOne.hasNext()) {
                            curAlignment.fromBytes( iterOne.next() );   
                            if (curAlignment.m_differences < bestAlignment.m_differences) {
                                bestAlignment.set(curAlignment);
                                secondBestAlignment = null;
                            } else if(curAlignment.m_differences == bestAlignment.m_differences) {
                                secondBestAlignment = new AlignmentRecord();
                                secondBestAlignment.set(curAlignment);  
                            }
                        }
                        
                        Iterator<BytesWritable> iterTwo = BytesListTwo.iterator();
                        while(iterTwo.hasNext()) {
                            curAlignment.fromBytes( iterTwo.next() );
                            if (curAlignment.m_differences < bestAlignment.m_differences) {
                                bestAlignment.set(curAlignment);
                                secondBestAlignment = null;
                            } else if(curAlignment.m_differences == bestAlignment.m_differences) {
                                secondBestAlignment = new AlignmentRecord();
                                secondBestAlignment.set(curAlignment);  
                            }
                        }
                        
                        res.add(bestAlignment.toBytes());
                        if(secondBestAlignment != null) {
                            res.add(secondBestAlignment.toBytes());
                        }
                        return res;
                    }
            })
            .flatMapToPair(
                new PairFlatMapFunction<Tuple2<IntWritable,Iterable<BytesWritable>>, IntWritable, BytesWritable>() {
                    /**
                     * Output the best alignment when it is unique
                     */
                    @Override
                    public Iterable<Tuple2<IntWritable, BytesWritable>> call(
                            Tuple2<IntWritable, Iterable<BytesWritable>> recordWithList) throws Exception {
                
                        int i = 0;
                        Tuple2<IntWritable, BytesWritable> firstAlignment = null;
                        Tuple2<IntWritable, BytesWritable> secondAlignment = null;
                        Iterator<BytesWritable> iter0 = recordWithList._2.iterator();
                        while(iter0.hasNext()) {
                            BytesWritable bw = iter0.next();
                            byte[] bytesCopied = new byte[bw.getLength()];
                            System.arraycopy(bw.getBytes(), 0, bytesCopied, 0, bytesCopied.length);
                            if(i == 0) {
                                firstAlignment = new Tuple2<IntWritable, BytesWritable>(new IntWritable(recordWithList._1.get()), new BytesWritable(bytesCopied));
                            } else if(i == 1) {
                                secondAlignment = new Tuple2<IntWritable, BytesWritable>(new IntWritable(recordWithList._1.get()), new BytesWritable(bytesCopied));
                            }
                            i += 1;
                        }
                        ArrayList<Tuple2<IntWritable, BytesWritable>> res = new ArrayList<Tuple2<IntWritable, BytesWritable>>();
                        if(secondAlignment == null) {
                            res.add(firstAlignment);
                        } else {
                            AlignmentRecord first = new AlignmentRecord();
                            AlignmentRecord second = new AlignmentRecord();
                            first.fromBytes(firstAlignment._2);
                            second.fromBytes(secondAlignment._2);
                            if(first.m_differences != second.m_differences) {
                                res.add(firstAlignment);
                            }
                        }
                        return res;
                    }
                });
        
        /**
         * Save the results to sequence file
         */
        unambiguousAlign.saveAsNewAPIHadoopFile(
                outputPath, 
                IntWritable.class, 
                BytesWritable.class, 
                SequenceFileOutputFormat.class);
        long timeEnd = System.currentTimeMillis();
        System.out.println("Filter time: " + (timeEnd - timeMid)/1000.0 + " seconds");
        System.out.println("Filter time: " + (timeEnd - timeStart)/1000.0 + " seconds");
        System.out.println("Output alignments number: " + unambiguousAlign.count());
        context.stop();
    }
}
