/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package models;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Indexed;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import play.modules.morphia.Model;

/**
 *
 * @author loopasam
 */
@Entity
public class MorphiaCitation extends Model {

    public MorphiaCitation(String pmid, String title, String abstractText, String created, String journalAbbreviation, String citationCount) throws ParseException {
        this.pmid = pmid;
        this.title = title;
        this.abstractText = abstractText;
        this.journalAbbreviation = journalAbbreviation;
        if (citationCount == null) {
            this.citationCount = 0;
        } else {
            this.citationCount = Integer.parseInt(citationCount);
        }
        this.created = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.S", Locale.ENGLISH).parse(created);
    }

    @Indexed
    public String pmid;

    public String title;

    public String abstractText;

    public Date created;

    //Indexed to build the impact factors
    @Indexed
    public String journalAbbreviation;

    public int citationCount;

}
