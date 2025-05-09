window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
    
        // update title variable name
        update_title_var: function(cat, met_var, hydro_var) {
            hydro_vars = {'swe_r': 'SWE Percentile (daily)', 'smtot_r': '2-m SM Percentile (daily)',
                          'modis_sca': 'MODIS Snow Cover'};
            met_vars   = {'precip': 'Precipitation (daily)', 'tair2m': 'Air Temperature (daily)', 
                          'precip_r': 'P Percentile (monthly)', 'tair2m_r': 'T Percentile (monthly)'};
            if (typeof this.varlabel == "undefined") {
                this.varlabel = "2-m SM Percentile (daily)";
            }
            if (cat=="met") {
                this.varlabel = met_vars[met_var];
            } else if (cat=="hydro") {
                this.varlabel = hydro_vars[hydro_var];
            }
            return this.varlabel;
        },
        
        // update title date
        update_title_date: function(date_value) {
            return ' @ ' + date_value;
        },
        
        // update title date
        update_title_hour: function(hour_value) {
            if (hour_value<10) {
                return '0' + hour_value + ':00Z';
            } else {
                return hour_value + ':00Z';
            }
        },
        
        // update overlay image url
        update_img_url: function(date_value, cat, met_var, hydro_var) {
            //var base_url = 'https://cw3e.ucsd.edu/wrf_hydro/cnrfc/imgs/';
            var base_url = 'https://storage.googleapis.com/cw3e-water-panel.appspot.com/imgs/';
            //var base_url = '/static/imgs/';
            var var_path = {'swe_r': 'cbrfc/output', 'smtot_r': 'cbrfc/output', 'precip': 'cbrfc/forcing', 'tair2m': 'cbrfc/forcing', 
                             'precip_r': 'cbrfc/forcing', 'tair2m_r': 'cbrfc/forcing', 'modis_sca': 'obs/modis'};
            if (typeof this.varname == "undefined") {
                this.varname = "smtot_r";
            }
            if (cat=="met") {
                this.varname = met_var;
            } else if (cat=="hydro") {
                this.varname = hydro_var;
            }
            
            var d = new Date(date_value);
            var yyyy = d.getUTCFullYear().toString();
            var mm = (d.getUTCMonth()+1).toString(); if (mm<10) { mm = '0' + mm; }
            var dd = d.getUTCDate().toString(); if (dd<10) { dd = '0' + dd; }
            if (this.varname=='precip_r' || this.varname=='tair2m_r') {
                dd = ''
            }
            
            var new_url = base_url + var_path[this.varname] + '/' + yyyy + '/' + this.varname + '_' + yyyy + mm + dd + '.png';
            return new_url;
        },
        
        // update overlay color bar
        update_cbar: function(cat, met_var, hydro_var) {
            //var base_url = 'https://cw3e.ucsd.edu/wrf_hydro/cnrfc/imgs/';
            var base_url = 'https://storage.googleapis.com/cw3e-water-panel.appspot.com/imgs/';
            //var base_url = '/static/imgs/';
            var var_path = {'swe_r': 'cbrfc/output', 'smtot_r': 'cbrfc/output', 'precip': 'cbrfc/forcing', 'tair2m': 'cbrfc/forcing', 
                             'precip_r': 'cbrfc/forcing', 'tair2m_r': 'cbrfc/forcing', 'modis_sca': 'obs/modis'};
            if (typeof this.varname == "undefined") {
                this.varname = "smtot_r";
            }
            if (cat=="met") {
                this.varname = met_var;
            } else if (cat=="hydro") {
                this.varname = hydro_var;
            }
            var new_url = base_url + var_path[this.varname] + '/' + this.varname + '_cbar.png';
            return new_url;
        },
        
        update_cbar_visibility: function(checked) {
            if (checked==false) {
                return {'display': 'none'};
            }
            else {
                return {'display': 'block'};
            }
        },
        
        // update datepicker and slider on button clicks
        update_date: function(dfwd_t, dbwd_t, mfwd_t, mbwd_t, d_old, d_min, d_max) {
        
            var date_old = new Date(d_old);
            
            var date_min =new Date(d_min);
            var date_max =new Date(d_max);
            
            var date_new = new Date();
            
            if (dfwd_t==null) { dfwd_t = 0; }
            if (dbwd_t==null) { dbwd_t = 0; }
            if (mfwd_t==null) { mfwd_t = 0; }
            if (mbwd_t==null) { mbwd_t = 0; }
            
            if (dfwd_t>dbwd_t && dfwd_t>mfwd_t && dfwd_t>mbwd_t) {
                // forward-day
                date_new.setTime(date_old.getTime() + 3600*1000*24);
            }
            else if (dbwd_t>dfwd_t && dbwd_t>mfwd_t && dbwd_t>mbwd_t) {
                // backward-day
                date_new.setTime(date_old.getTime() - 3600*1000*24);
            }
            else if (mfwd_t>dfwd_t && mfwd_t>dbwd_t && mfwd_t>mbwd_t) {
                // forward-month
                date_new.setTime(date_old.getTime() + 24*3600*1000*30.4375);
            }
            else if (mbwd_t>dfwd_t && mbwd_t>dbwd_t && mbwd_t>mfwd_t) {
                // backward-day
                date_new.setTime(date_old.getTime() - 24*3600*1000*30.4375);
            }
            else {
                date_new.setTime(date_old.getTime());
            }
            
            var timenow = Date.now();
            if (timenow-dfwd_t>100 && timenow-dbwd_t>100 && timenow-mfwd_t>100 && timenow-mbwd_t>100) {
                date_new.setTime(date_old.getTime());
            }
            
            if (date_new.getTime()<date_min.getTime()) {
                date_new = date_min;
            }
            if (date_new.getTime()>date_max.getTime()) {
                date_new = date_max;
            }
            
            return date_new.toISOString().split('T')[0];
            
        },
            
        // HUC sources according to zoom level
        switch_huc: function(zoom_level) {
            if (zoom_level<8) {
                return ['assets/huc4_cbrfc_0.5_tooltip.pbf', true];
            } else if (zoom_level<9) {
                return ['assets/huc6_cbrfc_0.7_tooltip.pbf', true];
            } else if (zoom_level<10) {
                return ['assets/huc8_cbrfc_1.0_tooltip.pbf', false];
            } else {
                return ['assets/huc10_cbrfc_1.0_tooltip.pbf', false];
            }
        },

        // switch river vector sources according to zoom level
        switch_river_vector: function(zoom_level, center) {
            if (zoom_level>=10 && (center['lat']>40 && center['lat']<41.5 && center['lng']>-111 && center['lng']<-106.5)) {
                return ['assets/nwm_reaches_yampa_snake_green_order2plus_0d001.pbf', false];
            } else {
                // return ['assets/nwm_reaches_cbrfc_order4plus_0d001_single_matched.pbf', true];
                return ['assets/nwm_reaches_cbrfc_order4plus_0d001.pbf', false];
            }
        },

        // switch region according to zoom level
        /*
        switch_region: function(zoom_level, center) {
            if (zoom_level>=8 && (center['lat']>40 && center['lat']<41.5 && center['lng']>-111 && center['lng']<-106.5)) {
                return ['assets/yampa_snake_green_region_0d001.pbf', false];
            } else {
                return ['assets/cbrfc_5.0.pbf', false];
            }
        },
        */

        // open the pop-up window for google doc
        open_gdoc: function(n_clicks) {
            return true;
        },

        // open the pop-up window for forcing doc
        open_fdoc: function(n_clicks) {
            return true;
        },

        // toggle collapse-openmore
        toggle_openmore: function(n_clicks, is_open) {
            if (n_clicks>0) {
                if (is_open) { return [!is_open, 'More »']; }
                else         { return [!is_open, 'Less «']; }
            }
            else {
                if (is_open) { return [is_open, 'Less «']; }
                else         { return [is_open, 'More »']; }
            }
        }

    }
});
