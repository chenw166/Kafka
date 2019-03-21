package com.cloud.dms;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("hello1")
public class HelloController {
	
   @RequestMapping("")
   public String hello() {
          return "helloworld1";
   }

}
