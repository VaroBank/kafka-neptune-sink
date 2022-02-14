/*
Copyright (c) 2022 Varo Bank, N.A. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package com.varobank.common.neptune.api;

import java.util.Map;

public class NeptuneInstanceProperties {

    private final String instanceId;
    private final String role;
    private final String endpoint;
    private final String status;
    private final String availabilityZone;
    private final String instanceType;

    private final Map<String, String> tags;

    public NeptuneInstanceProperties(String instanceId,
                                     String role,
                                     String endpoint,
                                     String status,
                                     String availabilityZone,
                                     String instanceType,
                                     Map<String, String> tags) {
        this.instanceId = instanceId;
        this.role = role;
        this.endpoint = endpoint;
        this.status = status;
        this.availabilityZone = availabilityZone;
        this.instanceType = instanceType;
        this.tags = tags;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getRole() {
        return role;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getStatus() {
        return status;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public boolean hasTag(String tag){
        return tags.containsKey(tag);
    }

    public String getTag(String tag){
        return tags.get(tag);
    }

    public String getTag(String tag, String defaultValue){
        if (!tags.containsKey(tag)){
            return defaultValue;
        }
        return tags.get(tag);
    }

    public boolean hasTag(String tag, String value){
        return hasTag(tag) && getTag(tag).equals(value);
    }

    public boolean isAvailable(){
        return getStatus().equalsIgnoreCase("Available");
    }

    public boolean isPrimary() {
        return getRole().equalsIgnoreCase("writer");
    }

    public boolean isReader() {
        return getRole().equalsIgnoreCase("reader");
    }

    @Override
    public String toString() {
        return "NeptuneInstanceProperties{" +
                "instanceId='" + instanceId + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", status='" + status + '\'' +
                ", availabilityZone='" + availabilityZone + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", tags=" + tags +
                '}';
    }
}
