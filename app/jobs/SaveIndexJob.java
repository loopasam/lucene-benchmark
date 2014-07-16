/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jobs;

import com.google.common.base.Stopwatch;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import models.MorphiaCitation;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import play.Logger;
import play.jobs.Job;
import play.modules.morphia.Model;
import play.vfs.VirtualFile;

/**
 *
 * @author loopasam
 */
public class SaveIndexJob extends Job {

    @Override
    public void doJob() throws Exception {
        Logger.info("Indexing articles...");
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        //Get the articles in array
        Path filePath = VirtualFile.fromRelativePath("/data/test_pubmed_ids.txt").getRealFile().toPath();
        Charset charset = Charset.defaultCharset();
        List<String> pmids = Files.readAllLines(filePath, charset);

        //TODO analyzer tests
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
        Directory directory = FSDirectory.open(VirtualFile.fromRelativePath("/index").getRealFile());
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);

        int total = pmids.size();
        int counter = 0;
        List<String> errors = new ArrayList<String>();

        for (String pmid : pmids) {
            counter++;
            Logger.info("pmid (" + pmid + "): " + counter + "/" + total);
            
            //Can be modified - gets the articles
            MorphiaCitation citation = MorphiaCitation.filter("pmid", pmid).first();
            Logger.info("query done");
            if (citation != null) {
                Document doc = new Document();
                String contents = "";
                
                doc.add(new Field("pmid", pmid, TextField.TYPE_STORED));

                if (citation.abstractText != null) {
                    contents += citation.abstractText;
                }

                if (citation.title != null) {
                    contents += citation.title;
                }

                if (!contents.equals("")) {
                    doc.add(new Field("contents", contents, TextField.TYPE_STORED));
                }

                iwriter.addDocument(doc);
            }else{
                errors.add(pmid);
                Logger.info("Document unkown: " + pmid);
            }

        }
        iwriter.close();
        stopwatch.stop();
        Logger.info("Time to index the documents: " + stopwatch.elapsed(TimeUnit.SECONDS));
        Logger.info("Errors: " + errors);
    }
}
