/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jobs;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import play.Logger;
import play.jobs.Job;
import play.vfs.VirtualFile;

/**
 *
 * @author loopasam
 */
public class EvaluateRetrieval extends Job {

    @Override
    public void doJob() throws Exception {

        Path filePath = VirtualFile.fromRelativePath("/data/mesh_disease_terms.txt").getRealFile().toPath();
        Charset charset = Charset.defaultCharset();
        List<String> lines = Files.readAllLines(filePath, charset);

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        int total = lines.size();
        int counter = 0;
        
        //TODO just to store the resutls and printing file
        StringBuilder sb = new StringBuilder();
        sb.append("PMID\tMESH_ID\tMESH_TERM\n");

        for (String line : lines) {
            String[] splits = line.split("\t");
            String id = splits[0];
            String term = splits[1];
            String originalTerm = splits[1];

            counter++;
            Logger.info("Term: " + term + "(" + counter + "/" + total + ")");
            
            if (term.contains(",")) {
                Pattern p = Pattern.compile("(.*), (.*)");
                Matcher m = p.matcher(term);
                if (m.find()) {
                    String post = m.group(1);
                    String pre = m.group(2);
                    term = pre + " " + post;
                    Logger.info("Term modified: " + term);
                }
            }

            Directory directory = FSDirectory.open(VirtualFile.fromRelativePath("/index").getRealFile());
            DirectoryReader ireader = DirectoryReader.open(directory);

            //TODO Query analyzer - can be changed and switched
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            
            //Maybe different type of parser?
            QueryParser parser = new QueryParser(Version.LUCENE_47, "contents", analyzer);

            // Logger.info("Query: " + term);
            if (!term.contains("(")) {

                //TODO different syntax and operators
                Query query = parser.parse("\"" + term.replace("/", "\\/") + "\"");
                //Logger.info("query: " + query.toString());

                ScoreDoc[] hits = isearcher.search(query, null, 10000).scoreDocs;
                //Logger.info("results: " + hits.length);
                int freq = hits.length;

                if (freq > 0) {
                    for (int i = 0; i < hits.length; i++) {
                        Document hitDoc = isearcher.doc(hits[i].doc);
                        //Logger.info(hitDoc.get("pmid") + " - " + hits[i].score);
                        sb.append(hitDoc.get("pmid")).append("\t").append(id).append("\t").append(originalTerm).append("\n");
                    }
                }

            }
            ireader.close();
            directory.close();

        }
        stopwatch.stop();
        Logger.info("Time to index the documents: " + stopwatch.elapsed(TimeUnit.SECONDS));
        File file = VirtualFile.fromRelativePath("/data/annotatedArticles.txt").getRealFile();
        FileUtils.writeStringToFile(file, sb.toString());
        Logger.info("File saved: " + file.getAbsolutePath());
    }

}
