/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.wtiger001.brigade.framework.web;

import com.github.wtiger001.brigade.framework.Framework;
import com.github.wtiger001.brigade.framework.KafkaInput;
import java.util.Collection;
import java.util.HashSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author bmcdonie
 */
@RestController
public class BrigadeWebController {

    @Autowired
    private Framework framework;

    @Autowired
    private KafkaInput kafkaInput;

    @RequestMapping("/trackers")
    public @ResponseBody
    Collection getTrackers() {
        return new HashSet(kafkaInput.getTracker().getTaskIndex().values());
    }

    @RequestMapping("/status")
    public ResponseEntity getStatus() {
        if (framework.threadsAlive()) {
            return new ResponseEntity("OK", HttpStatus.OK);
        } else {
            return new ResponseEntity("One or more Brigade threads have died", HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

}
