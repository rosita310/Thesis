package parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Member {

    private final static Logger LOGGER = LoggerFactory.getLogger(Member.class);

    // The closing parenthesis is missing when the affiliation wraps to the next line
    private final static Pattern affiliationPattern = Pattern.compile("\\(([^\\)]*)\\)?$");
    // "Eli Biham . . . . . . Technion"
    private final static Pattern dotLeaderPattern = Pattern.compile("\\s*(\\.\\s*){4,}");

    private final String name;
    private String affiliation;
    private String role;
    private String firstname;

    private String lastname;

    public Member(String name) {
        name = name.trim().replaceAll("\\s\\s+", " ");
        Matcher leader = dotLeaderPattern.matcher(name);
        if (leader.find()) {
            String rest = name.substring(leader.end()).trim();
            if (!rest.isEmpty()) {
                this.affiliation = rest;
            }
            name = name.substring(0, leader.start()).trim();
        }
        Matcher matches = affiliationPattern.matcher(name);
        if(matches.find()) {
            this.affiliation = matches.group(1).trim();
            name = name.substring(0, matches.start()).trim();
        }

        if (name.contains(",") && name.split(",").length >= 2) {
            String[] parts = name.split(",");
            this.firstname = parts[1].trim();
            this.lastname = parts[0].trim();
            
            if(parts.length > 2) {
                if(this.affiliation == null) {
                    this.name = firstname + ' ' + lastname;
                    this.affiliation = name.substring(parts[0].length() + 1 + parts[1].length() + 1).trim();
                } else{
                    this.name = name;
                }
            } else {
                this.name = firstname + ' ' + lastname;
            }
        } else {
            this.name = name;
            String[] parts = name.split(" ");
            if(parts.length >= 2) {
                this.firstname = parts[0];
                this.lastname = parts[parts.length - 1];
            }
        }
    }

    public Member(String firstname, String lastname) {
        this.lastname = lastname;
        this.firstname = firstname;
        this.name = firstname + " " + lastname;
    }

    public String getName() {
        return this.name;
    }

    public String getFirstname() {
        return this.firstname;
    }


    public String getAffiliation() {
        return affiliation;
    }

    public String getRole() {
        return role;
    }

    public String getLastname() {
        return lastname;
    }

    public void setAffiliation(String affiliation) {
        this.affiliation = affiliation;
    }

    public void setRole(String role) {
        if (this.role == null) {
            this.role = role;
        } else {
            LOGGER.trace("Preventing existing role {} to be overridden with value: {}", this.role, role);
        }
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        String jsonString = gson.toJson(this);
        return jsonString;
    }

}
