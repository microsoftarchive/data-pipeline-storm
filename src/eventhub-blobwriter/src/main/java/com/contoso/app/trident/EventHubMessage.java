// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.io.Serializable;
public class EventHubMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public double lat;
    public double lng;
    public long time;
    public String id;
    public EventHubMessage (double lat, double lng, long time, String id) {
        super();
        this.time = time;
        this.lat = lat;
        this.lng = lng;
        this.id = id;
    }
}
