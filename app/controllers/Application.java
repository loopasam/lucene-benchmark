package controllers;

import java.util.*;
import jobs.EvaluateRetrieval;
import jobs.SaveIndexJob;
import play.*;
import play.mvc.*;


public class Application extends Controller {

    public static void index() {
        render();
    }
    
    public static void indexText() {
        new SaveIndexJob().now();
        index();
    }

    public static void query() {
        new EvaluateRetrieval().now();
        index();
    }
}